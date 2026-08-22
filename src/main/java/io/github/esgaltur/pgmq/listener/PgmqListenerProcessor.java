package io.github.esgaltur.pgmq.listener;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.github.esgaltur.pgmq.annotation.PgmqListener;
import io.github.esgaltur.pgmq.config.PgmqProperties;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.core.PgmqMessage;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

@Slf4j
@RequiredArgsConstructor
public class PgmqListenerProcessor implements SmartLifecycle {

    private final PgmqListenerRegistrar listenerRegistrar;
    private final PgmqTemplate pgmqTemplate;
    private final PgmqIdempotencyRepository idempotencyRepository;
    private final PgmqProperties pgmqProperties;
    private final PgmqListenerWakeupStrategy wakeupStrategy;
    private final MeterRegistry meterRegistry; // Optional

    private final ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
    private final List<Future<?>> futures = new ArrayList<>();
    private volatile boolean running = false;

    @Override
    public void start() {
        List<PgmqListenerMetadata> listeners = listenerRegistrar.listeners();
        if (this.running) {
            log.debug("PGMQ listener processor is already running; duplicate start request ignored.");
            return;
        }
        if (listeners.isEmpty()) {
            log.debug("No @PgmqListener methods were registered; listener processor will not start.");
            return;
        }

        Set<String> uniqueQueues = new HashSet<>();
        Set<String> listenerQueues = new HashSet<>();

        // Auto-create queues before starting consumer workers.
        if (pgmqProperties.isAutoCreateQueue()) {
            for (PgmqListenerMetadata metadata : listeners) {
                listenerQueues.add(metadata.queue());
                try {
                    pgmqTemplate.createQueue(metadata.queue());
                    uniqueQueues.add(metadata.queue());
                    log.debug("Ensured PGMQ queue {} exists.", metadata.queue());
                    
                    if (metadata.deadLetterQueue() != null && !metadata.deadLetterQueue().isEmpty()) {
                        pgmqTemplate.createQueue(metadata.deadLetterQueue());
                        uniqueQueues.add(metadata.deadLetterQueue());
                        log.debug("Ensured PGMQ dead-letter queue {} exists.", metadata.deadLetterQueue());
                    }
                } catch (Exception e) {
                    log.warn("Could not auto-create queues for {}.", metadata.queue(), e);
                }
            }
        } else {
            for (PgmqListenerMetadata metadata : listeners) {
                uniqueQueues.add(metadata.queue());
                listenerQueues.add(metadata.queue());
            }
        }

        wakeupStrategy.start(listenerQueues);

        // Register Queue Depth Metrics
        if (meterRegistry != null) {
            for (String queue : uniqueQueues) {
                Gauge.builder("pgmq.queue.depth", () -> pgmqTemplate.getQueueDepth(queue))
                     .tag("queue", queue)
                     .description("Current number of messages in the PGMQ queue")
                     .register(meterRegistry);
            }
        }

        int totalThreads = listeners.stream().mapToInt(l -> Math.max(1, l.concurrency())).sum();
        taskScheduler.setPoolSize(Math.max(1, totalThreads));
        taskScheduler.setThreadNamePrefix("pgmq-listener-");
        taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        taskScheduler.setAwaitTerminationSeconds((int) pgmqProperties.getShutdownTimeout().getSeconds());
        taskScheduler.initialize();

        this.running = true;
        for (PgmqListenerMetadata metadata : listeners) {
            int concurrency = Math.max(1, metadata.concurrency());
            for (int i = 0; i < concurrency; i++) {
                PgmqListenerWakeupStrategy.WaitHandle waitHandle = wakeupStrategy.createWaitHandle(
                        metadata.queue(), Duration.ofMillis(metadata.annotation().pollInterval()));
                Future<?> future = taskScheduler.submit(() -> consumeLoop(metadata, waitHandle));
                futures.add(future);
            }
        }

        log.info("PGMQ Listener Processor started with {} consumer worker(s) in {} mode.",
                totalThreads, wakeupStrategy.description());
    }

    private void consumeLoop(
            PgmqListenerMetadata metadata,
            PgmqListenerWakeupStrategy.WaitHandle waitHandle) {
        while (running && !Thread.currentThread().isInterrupted()) {
            long observedGeneration = waitHandle.snapshot();
            boolean foundMessages = pollAndInvoke(metadata);
            if (foundMessages) {
                continue;
            }

            try {
                waitHandle.awaitChange(observedGeneration);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean pollAndInvoke(PgmqListenerMetadata metadata) {
        String queue = metadata.queue();
        int vt = metadata.annotation().vt();
        int qty = metadata.annotation().qty();
        Class<?> payloadType = metadata.payloadType();

        try {
            List<?> rawMessages = pgmqTemplate.read(queue, vt, qty, payloadType);
            if (rawMessages.isEmpty()) return false;
            log.debug("Read {} message(s) from PGMQ queue {}.", rawMessages.size(), queue);

            List<PgmqMessage<?>> batchToProcess = new ArrayList<>();

            for (Object obj : rawMessages) {
                PgmqMessage<?> msg = (PgmqMessage<?>) obj;
                
                try {
                    // Check DLQ / Max Retries
                    int maxRetries = metadata.annotation().maxRetries();
                    if (maxRetries > 0 && msg.getReadCount() > maxRetries) {
                        log.warn("Message {} from {} exceeded max retries ({}). Routing to DLQ.", msg.getMsgId(), queue, maxRetries);
                        if (metadata.deadLetterQueue() != null && !metadata.deadLetterQueue().isEmpty()) {
                            pgmqTemplate.send(metadata.deadLetterQueue(), msg.getPayload());
                        }
                        archiveOrDelete(metadata.annotation(), queue, msg.getMsgId());
                        recordMetric("dlq", queue);
                        continue;
                    }

                    // Check Idempotency
                    if (metadata.annotation().idempotent()) {
                        if (idempotencyRepository.isProcessed(queue, msg.getMsgId())) {
                            log.debug("Message {} from queue {} already processed. Skipping.", msg.getMsgId(), queue);
                            archiveOrDelete(metadata.annotation(), queue, msg.getMsgId());
                            continue;
                        }
                    }

                    batchToProcess.add(msg);
                } catch (Exception e) {
                    log.error("Error evaluating message {} from queue {}.", msg.getMsgId(), queue, e);
                }
            }

            if (batchToProcess.isEmpty()) return true;

            Timer.Sample sample = null;
            if (meterRegistry != null) {
                sample = Timer.start(meterRegistry);
            }

            try {
                if (metadata.batch()) {
                    processBatch(metadata, queue, batchToProcess);
                } else {
                    processSequentially(metadata, queue, batchToProcess);
                }

                if (sample != null) {
                    sample.stop(meterRegistry.timer("pgmq.listener.latency", "queue", queue, "status", "success"));
                }
            } catch (Exception processException) {
                if (sample != null) {
                    sample.stop(meterRegistry.timer("pgmq.listener.latency", "queue", queue, "status", "failure"));
                }
                throw processException;
            }

        } catch (Exception e) {
            // General polling failure
            log.error("Error polling PGMQ queue {}.", queue, e);
            return false;
        }
        return true;
    }

    private void processBatch(PgmqListenerMetadata metadata, String queue, List<PgmqMessage<?>> batch) {
        try {
            Object argument;
            if (metadata.messageWrapped()) {
                argument = batch;
            } else {
                List<Object> payloads = new ArrayList<>();
                for (PgmqMessage<?> m : batch) {
                    payloads.add(m.getPayload());
                }
                argument = payloads;
            }

            metadata.method().invoke(metadata.bean(), argument);

            for (PgmqMessage<?> msg : batch) {
                markIdempotentAndArchive(metadata.annotation(), queue, msg.getMsgId());
                recordMetric("success", queue);
            }
        } catch (Exception e) {
            log.error("Error processing batch for queue {}.", queue, e);
            handleBackoffForBatch(metadata, queue, batch);
        }
    }

    private void processSequentially(PgmqListenerMetadata metadata, String queue, List<PgmqMessage<?>> batch) {
        for (PgmqMessage<?> msg : batch) {
            try {
                if (metadata.messageWrapped()) {
                    metadata.method().invoke(metadata.bean(), msg);
                } else {
                    metadata.method().invoke(metadata.bean(), msg.getPayload());
                }
                markIdempotentAndArchive(metadata.annotation(), queue, msg.getMsgId());
                recordMetric("success", queue);
            } catch (Exception e) {
                log.error("Error processing message {} from queue {}.", msg.getMsgId(), queue, e);
                handleBackoff(metadata, queue, msg);
                recordMetric("failure", queue);
            }
        }
    }

    private void handleBackoffForBatch(PgmqListenerMetadata metadata, String queue, List<PgmqMessage<?>> batch) {
        for (PgmqMessage<?> msg : batch) {
            handleBackoff(metadata, queue, msg);
            recordMetric("failure", queue);
        }
    }

    private void handleBackoff(PgmqListenerMetadata metadata, String queue, PgmqMessage<?> msg) {
        double multiplier = metadata.annotation().backoffMultiplier();
        if (multiplier > 1.0) {
            try {
                int baseVt = metadata.annotation().vt();
                int retries = msg.getReadCount(); // Number of times it has been read
                long calculatedVt = Math.round(baseVt * Math.pow(multiplier, retries));
                int newVt = (int) Math.min(calculatedVt, metadata.annotation().maxBackoff());
                
                pgmqTemplate.setVt(queue, msg.getMsgId(), newVt);
                log.debug("Applied exponential backoff. Message {} on queue {} next visible in {} seconds.", msg.getMsgId(), queue, newVt);
            } catch (Exception e) {
                log.warn("Failed to apply exponential backoff for message {} on queue {}.",
                        msg.getMsgId(), queue, e);
            }
        }
    }

    private void recordMetric(String status, String queue) {
        if (meterRegistry != null) {
            meterRegistry.counter("pgmq.messages.processed", "queue", queue, "status", status).increment();
        }
    }

    private void markIdempotentAndArchive(PgmqListener annotation, String queue, long msgId) {
        if (annotation.idempotent()) {
            idempotencyRepository.markProcessed(queue, msgId);
        }
        archiveOrDelete(annotation, queue, msgId);
    }

    private void archiveOrDelete(PgmqListener annotation, String queue, long msgId) {
        if (annotation.archive()) {
            pgmqTemplate.archive(queue, msgId);
        } else {
            pgmqTemplate.delete(queue, msgId);
        }
    }

    @Override
    public void stop() {
        if (!this.running) return;
        
        log.info("Initiating graceful shutdown of PGMQ listeners. Stopping consumer workers...");
        this.running = false;

        wakeupStrategy.close();
        futures.forEach(f -> f.cancel(false));
        futures.clear();
        
        taskScheduler.shutdown();
        
        log.info("PGMQ Listener Processor successfully stopped.");
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

}
