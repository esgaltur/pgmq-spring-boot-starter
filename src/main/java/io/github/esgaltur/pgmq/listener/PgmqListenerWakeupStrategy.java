package io.github.esgaltur.pgmq.listener;

import java.time.Duration;
import java.util.Set;

/**
 * Strategy used by listener workers to wait when their queue is empty.
 */
public interface PgmqListenerWakeupStrategy extends AutoCloseable {

    void start(Set<String> queues);

    WaitHandle createWaitHandle(String queue, Duration pollingInterval);

    String description();

    @Override
    void close();

    interface WaitHandle {
        long snapshot();

        void awaitChange(long observedGeneration) throws InterruptedException;
    }
}
