package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListenerMode;
import io.github.esgaltur.pgmq.config.PgmqProperties;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

@Slf4j
@RequiredArgsConstructor
public class PgmqListenerProcessor implements SmartLifecycle {

    private final PgmqListenerRegistrar listenerRegistrar;
    private final PgmqTemplate pgmqTemplate;
    private final PgmqMessageHandler messageHandler;
    private final PgmqProperties pgmqProperties;
    private final PgmqListenerWakeupStrategy wakeupStrategy;
    private final PgmqListenerStatus listenerStatus;
    private final PgmqListenerMetrics listenerMetrics;

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
        Map<String, PgmqListenerMode> listenerQueues = listeners.stream()
                .collect(java.util.stream.Collectors.toMap(
                        PgmqListenerMetadata::queue,
                        this::resolveListenerMode,
                        this::requireCompatibleModes));

        // Auto-create queues before starting consumer workers.
        if (pgmqProperties.isAutoCreateQueue()) {
            for (PgmqListenerMetadata metadata : listeners) {
                try {
                    pgmqTemplate.createQueue(metadata.queue());
                    uniqueQueues.add(metadata.queue());
                    log.debug("Ensured PGMQ queue {} exists.", metadata.queue());
                    
                    if (metadata.deadLetterQueue() != null && !metadata.deadLetterQueue().isEmpty()) {
                        pgmqTemplate.createQueue(metadata.deadLetterQueue());
                        uniqueQueues.add(metadata.deadLetterQueue());
                        log.debug("Ensured PGMQ dead-letter queue {} exists.", metadata.deadLetterQueue());
                    }
                } catch (Exception exception) {
                    log.warn("Could not auto-create PGMQ queue {}: {}",
                            metadata.queue(), exception.getMessage());
                    log.debug("PGMQ queue auto-creation failure for {}.", metadata.queue(), exception);
                }
            }
        } else {
            for (PgmqListenerMetadata metadata : listeners) {
                uniqueQueues.add(metadata.queue());
            }
        }

        wakeupStrategy.start(listenerQueues);

        for (String queue : uniqueQueues) {
            listenerMetrics.registerQueue(queue, () -> pgmqTemplate.getQueueDepth(queue));
        }

        int totalThreads = listeners.stream().mapToInt(l -> Math.max(1, l.concurrency())).sum();
        taskScheduler.setPoolSize(Math.max(1, totalThreads));
        taskScheduler.setThreadNamePrefix("pgmq-listener-");
        taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        taskScheduler.setAwaitTerminationSeconds((int) pgmqProperties.getShutdownTimeout().getSeconds());
        taskScheduler.initialize();

        this.running = true;
        listenerStatus.processorStarted();
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
                Optional<Duration> nextVisibleDelay = nextVisibleDelay(metadata.queue());
                PgmqListenerWakeupStrategy.WakeupReason reason = waitHandle.awaitChange(
                        observedGeneration, nextVisibleDelay);
                if (running) {
                    listenerStatus.wakeup(reason);
                }
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
            listenerStatus.queueRead();
            List<?> rawMessages = pgmqTemplate.read(queue, vt, qty, payloadType);
            if (rawMessages.isEmpty()) {
                listenerStatus.emptyRead();
                return false;
            }
            log.debug("Read {} message(s) from PGMQ queue {}.", rawMessages.size(), queue);

            List<PgmqMessage<?>> batchToProcess = new ArrayList<>();

            for (Object obj : rawMessages) {
                PgmqMessage<?> msg = (PgmqMessage<?>) obj;
                
                try {
                    // Check DLQ / Max Retries
                    int maxRetries = metadata.annotation().maxRetries();
                    if (maxRetries > 0 && msg.getReadCount() > maxRetries) {
                        log.warn("Message {} from {} exceeded max retries ({}). Routing to DLQ.", msg.getMsgId(), queue, maxRetries);
                        messageHandler.routeToDeadLetter(metadata, msg);
                        recordMetric("dlq", queue);
                        continue;
                    }

                    batchToProcess.add(msg);
                } catch (Exception e) {
                    log.error("Error evaluating message {} from queue {}.", msg.getMsgId(), queue, e);
                }
            }

            if (batchToProcess.isEmpty()) return true;

            long processingStartedAt = System.nanoTime();
            boolean successful = metadata.batch()
                    ? processBatch(metadata, queue, batchToProcess)
                    : processSequentially(metadata, queue, batchToProcess);
            listenerMetrics.processingCompleted(
                    queue,
                    successful ? "success" : "failure",
                    Duration.ofNanos(System.nanoTime() - processingStartedAt));

        } catch (Exception e) {
            // General polling failure
            listenerStatus.pollFailure();
            log.error("Error polling PGMQ queue {}.", queue, e);
            return false;
        }
        return true;
    }

    private boolean processBatch(PgmqListenerMetadata metadata, String queue, List<PgmqMessage<?>> batch) {
        try {
            int processed = messageHandler.processBatch(metadata, batch);
            for (int index = 0; index < processed; index++) {
                recordMetric("success", queue);
            }
            return true;
        } catch (Exception e) {
            log.error("Error processing batch for queue {}.", queue, e);
            handleBackoffForBatch(metadata, queue, batch);
            return false;
        }
    }

    private boolean processSequentially(PgmqListenerMetadata metadata, String queue, List<PgmqMessage<?>> batch) {
        boolean successful = true;
        for (PgmqMessage<?> msg : batch) {
            try {
                PgmqMessageHandler.Outcome outcome = messageHandler.processSingle(metadata, msg);
                if (outcome == PgmqMessageHandler.Outcome.DUPLICATE) {
                    log.debug("Message {} from queue {} was already processed. Skipping.",
                            msg.getMsgId(), queue);
                } else {
                    recordMetric("success", queue);
                }
            } catch (Exception e) {
                successful = false;
                log.error("Error processing message {} from queue {}.", msg.getMsgId(), queue, e);
                handleBackoff(metadata, queue, msg);
                recordMetric("failure", queue);
            }
        }
        return successful;
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
        listenerMetrics.messageProcessed(queue, status);
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
        listenerStatus.processorStopped();
        
        log.info("PGMQ Listener Processor successfully stopped.");
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    private Optional<Duration> nextVisibleDelay(String queue) {
        if (!pgmqProperties.isScheduleDelayedMessages()) {
            return Optional.empty();
        }
        try {
            return pgmqTemplate.getNextVisibleDelay(queue);
        } catch (Exception exception) {
            log.debug("Could not inspect the next visibility timestamp for queue {}.", queue, exception);
            return Optional.empty();
        }
    }

    private PgmqListenerMode resolveListenerMode(PgmqListenerMetadata metadata) {
        if (metadata.annotation().mode() != PgmqListenerMode.DEFAULT) {
            return metadata.annotation().mode();
        }
        return pgmqProperties.getListenerMode() == PgmqProperties.ListenerMode.NOTIFY
                ? PgmqListenerMode.NOTIFY
                : PgmqListenerMode.POLLING;
    }

    private PgmqListenerMode requireCompatibleModes(
            PgmqListenerMode first,
            PgmqListenerMode second) {
        if (first != second) {
            throw new IllegalStateException(
                    "Listeners sharing one PGMQ queue must use the same wake-up mode");
        }
        return first;
    }

}
