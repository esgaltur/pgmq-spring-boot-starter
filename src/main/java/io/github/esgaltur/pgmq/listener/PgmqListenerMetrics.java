package io.github.esgaltur.pgmq.listener;

import java.time.Duration;
import java.util.function.LongSupplier;

/** Observability port for listener metrics implementations. */
public interface PgmqListenerMetrics {

    PgmqListenerMetrics NO_OP = new PgmqListenerMetrics() {
    };

    default void registerQueue(String queue, LongSupplier depthSupplier) {
    }

    default void messageProcessed(String queue, String status) {
    }

    default void processingCompleted(String queue, String status, Duration duration) {
    }
}
