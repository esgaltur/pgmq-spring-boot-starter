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
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ResolvableType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@RequiredArgsConstructor
public class PgmqListenerProcessor implements BeanPostProcessor, SmartLifecycle, EmbeddedValueResolverAware {

    private final PgmqTemplate pgmqTemplate;
    private final PgmqIdempotencyRepository idempotencyRepository;
    private final PgmqProperties pgmqProperties;
    private final MeterRegistry meterRegistry; // Optional
    private StringValueResolver resolver;

    private final List<ListenerMetadata> listeners = new ArrayList<>();
    private final ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
    private final List<ScheduledFuture<?>> futures = new ArrayList<>();
    private boolean running = false;

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = bean.getClass();
        ReflectionUtils.doWithMethods(targetClass, method -> {
            PgmqListener annotation = method.getAnnotation(PgmqListener.class);
            if (annotation != null) {
                if (method.getParameterCount() != 1) {
                    throw new IllegalArgumentException("@PgmqListener method must have exactly one parameter");
                }
                
                ResolvableType resolvableType = ResolvableType.forMethodParameter(method, 0);
                Class<?> payloadType;
                boolean isMessageWrapped = false;
                boolean isBatch = List.class.isAssignableFrom(resolvableType.resolve(Object.class));
                
                ResolvableType targetType = isBatch ? resolvableType.getGeneric(0) : resolvableType;

                if (PgmqMessage.class.isAssignableFrom(targetType.resolve(Object.class))) {
                    isMessageWrapped = true;
                    payloadType = targetType.getGeneric(0).resolve(Object.class);
                } else {
                    payloadType = targetType.resolve(Object.class);
                }

                // Resolve SpEL expressions
                String resolvedQueue = resolver != null ? resolver.resolveStringValue(annotation.queue()) : annotation.queue();
                String resolvedDlq = resolver != null ? resolver.resolveStringValue(annotation.deadLetterQueue()) : annotation.deadLetterQueue();
                String concurrencyStr = resolver != null ? resolver.resolveStringValue(annotation.concurrency()) : annotation.concurrency();
                int concurrency = Integer.parseInt(concurrencyStr != null ? concurrencyStr : "1");

                listeners.add(new ListenerMetadata(bean, method, annotation, payloadType, isMessageWrapped, isBatch, resolvedQueue, resolvedDlq, concurrency));
                log.info("Registered PGMQ listener on method {} for queue {} (Batch: {}, Type: {}, Concurrency: {})", 
                         method.getName(), resolvedQueue, isBatch, payloadType.getSimpleName(), concurrency);
            }
        });
        return bean;
    }

    @Override
    public void start() {
        if (this.running || listeners.isEmpty()) return;

        Set<String> uniqueQueues = new HashSet<>();

        // Auto-create queues before starting the polling threads
        if (pgmqProperties.isAutoCreateQueue()) {
            for (ListenerMetadata metadata : listeners) {
                try {
                    pgmqTemplate.createQueue(metadata.resolvedQueue);
                    uniqueQueues.add(metadata.resolvedQueue);
                    
                    if (metadata.resolvedDlq != null && !metadata.resolvedDlq.isEmpty()) {
                        pgmqTemplate.createQueue(metadata.resolvedDlq);
                        uniqueQueues.add(metadata.resolvedDlq);
                    }
                } catch (Exception e) {
                    log.warn("Could not auto-create queues for {}: {}", metadata.resolvedQueue, e.getMessage());
                }
            }
        } else {
            for (ListenerMetadata metadata : listeners) {
                uniqueQueues.add(metadata.resolvedQueue);
            }
        }

        // Register Queue Depth Metrics
        if (meterRegistry != null) {
            for (String queue : uniqueQueues) {
                Gauge.builder("pgmq.queue.depth", () -> pgmqTemplate.getQueueDepth(queue))
                     .tag("queue", queue)
                     .description("Current number of messages in the PGMQ queue")
                     .register(meterRegistry);
            }
        }

        int totalThreads = listeners.stream().mapToInt(l -> Math.max(1, l.resolvedConcurrency)).sum();
        taskScheduler.setPoolSize(Math.max(1, totalThreads));
        taskScheduler.setThreadNamePrefix("pgmq-listener-");
        taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        taskScheduler.setAwaitTerminationSeconds((int) pgmqProperties.getShutdownTimeout().getSeconds());
        taskScheduler.initialize();

        for (ListenerMetadata metadata : listeners) {
            int concurrency = Math.max(1, metadata.resolvedConcurrency);
            for (int i = 0; i < concurrency; i++) {
                ScheduledFuture<?> future = taskScheduler.scheduleWithFixedDelay(
                    () -> pollAndInvoke(metadata),
                    Duration.ofMillis(metadata.annotation.pollInterval())
                );
                futures.add(future);
            }
        }

        this.running = true;
        log.info("PGMQ Listener Processor started with {} total concurrent polling tasks.", totalThreads);
    }

    @SuppressWarnings("unchecked")
    private void pollAndInvoke(ListenerMetadata metadata) {
        String queue = metadata.resolvedQueue;
        int vt = metadata.annotation.vt();
        int qty = metadata.annotation.qty();
        Class<?> payloadType = metadata.payloadType;

        try {
            List<?> rawMessages = pgmqTemplate.read(queue, vt, qty, payloadType);
            if (rawMessages.isEmpty()) return;

            List<PgmqMessage<?>> batchToProcess = new ArrayList<>();

            for (Object obj : rawMessages) {
                PgmqMessage<?> msg = (PgmqMessage<?>) obj;
                
                try {
                    // Check DLQ / Max Retries
                    int maxRetries = metadata.annotation.maxRetries();
                    if (maxRetries > 0 && msg.getReadCount() > maxRetries) {
                        log.warn("Message {} from {} exceeded max retries ({}). Routing to DLQ.", msg.getMsgId(), queue, maxRetries);
                        if (metadata.resolvedDlq != null && !metadata.resolvedDlq.isEmpty()) {
                            pgmqTemplate.send(metadata.resolvedDlq, msg.getPayload());
                        }
                        archiveOrDelete(metadata.annotation, queue, msg.getMsgId());
                        recordMetric("dlq", queue);
                        continue;
                    }

                    // Check Idempotency
                    if (metadata.annotation.idempotent()) {
                        if (idempotencyRepository.isProcessed(queue, msg.getMsgId())) {
                            log.debug("Message {} from queue {} already processed. Skipping.", msg.getMsgId(), queue);
                            archiveOrDelete(metadata.annotation, queue, msg.getMsgId());
                            continue;
                        }
                    }

                    batchToProcess.add(msg);
                } catch (Exception e) {
                    log.error("Error evaluating message {} from queue {}: {}", msg.getMsgId(), queue, e.getMessage());
                }
            }

            if (batchToProcess.isEmpty()) return;

            Timer.Sample sample = null;
            if (meterRegistry != null) {
                sample = Timer.start(meterRegistry);
            }

            try {
                if (metadata.isBatch) {
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
            log.error("Error polling PGMQ queue {}: {}", queue, e.getMessage());
        }
    }

    private void processBatch(ListenerMetadata metadata, String queue, List<PgmqMessage<?>> batch) {
        try {
            Object argument;
            if (metadata.isMessageWrapped) {
                argument = batch;
            } else {
                List<Object> payloads = new ArrayList<>();
                for (PgmqMessage<?> m : batch) {
                    payloads.add(m.getPayload());
                }
                argument = payloads;
            }

            metadata.method.invoke(metadata.bean, argument);

            for (PgmqMessage<?> msg : batch) {
                markIdempotentAndArchive(metadata.annotation, queue, msg.getMsgId());
                recordMetric("success", queue);
            }
        } catch (Exception e) {
            log.error("Error processing batch for queue {}: {}", queue, e.getMessage());
            handleBackoffForBatch(metadata, queue, batch);
        }
    }

    private void processSequentially(ListenerMetadata metadata, String queue, List<PgmqMessage<?>> batch) {
        for (PgmqMessage<?> msg : batch) {
            try {
                if (metadata.isMessageWrapped) {
                    metadata.method.invoke(metadata.bean, msg);
                } else {
                    metadata.method.invoke(metadata.bean, msg.getPayload());
                }
                markIdempotentAndArchive(metadata.annotation, queue, msg.getMsgId());
                recordMetric("success", queue);
            } catch (Exception e) {
                log.error("Error processing message {} from queue {}: {}", msg.getMsgId(), queue, e.getMessage());
                handleBackoff(metadata, queue, msg);
                recordMetric("failure", queue);
            }
        }
    }

    private void handleBackoffForBatch(ListenerMetadata metadata, String queue, List<PgmqMessage<?>> batch) {
        for (PgmqMessage<?> msg : batch) {
            handleBackoff(metadata, queue, msg);
            recordMetric("failure", queue);
        }
    }

    private void handleBackoff(ListenerMetadata metadata, String queue, PgmqMessage<?> msg) {
        double multiplier = metadata.annotation.backoffMultiplier();
        if (multiplier > 1.0) {
            try {
                int baseVt = metadata.annotation.vt();
                int retries = msg.getReadCount(); // Number of times it has been read
                long calculatedVt = Math.round(baseVt * Math.pow(multiplier, retries));
                int newVt = (int) Math.min(calculatedVt, metadata.annotation.maxBackoff());
                
                pgmqTemplate.setVt(queue, msg.getMsgId(), newVt);
                log.debug("Applied exponential backoff. Message {} on queue {} next visible in {} seconds.", msg.getMsgId(), queue, newVt);
            } catch (Exception e) {
                log.warn("Failed to apply exponential backoff for message {} on queue {}: {}", msg.getMsgId(), queue, e.getMessage());
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
        
        log.info("Initiating graceful shutdown of PGMQ listeners. Stopping new polling...");
        futures.forEach(f -> f.cancel(false));
        
        taskScheduler.shutdown();
        
        this.running = false;
        log.info("PGMQ Listener Processor successfully stopped.");
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @RequiredArgsConstructor
    private static class ListenerMetadata {
        final Object bean;
        final Method method;
        final PgmqListener annotation;
        final Class<?> payloadType;
        final boolean isMessageWrapped;
        final boolean isBatch;
        final String resolvedQueue;
        final String resolvedDlq;
        final int resolvedConcurrency;
    }
}
