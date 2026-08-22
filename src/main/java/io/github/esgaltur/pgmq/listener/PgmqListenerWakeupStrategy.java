package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListenerMode;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Strategy used by listener workers to wait when their queue is empty.
 */
public interface PgmqListenerWakeupStrategy extends AutoCloseable {

    void start(Map<String, PgmqListenerMode> queues);

    WaitHandle createWaitHandle(String queue, Duration pollingInterval);

    String description();

    @Override
    void close();

    enum WakeupReason {
        NOTIFICATION,
        CONFIRMATION,
        RECOVERY,
        SCHEDULED,
        POLLING
    }

    interface WaitHandle {
        long snapshot();

        WakeupReason awaitChange(
                long observedGeneration,
                Optional<Duration> nextVisibleDelay) throws InterruptedException;
    }
}
