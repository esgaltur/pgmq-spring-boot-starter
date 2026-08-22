package io.github.esgaltur.pgmq.annotation;

/**
 * Selects how an idle listener waits before its next durable PGMQ read.
 */
public enum PgmqListenerMode {
    /** Inherit {@code spring.pgmq.listener-mode}. */
    DEFAULT,
    /** Use PostgreSQL LISTEN/NOTIFY with timed recovery reads. */
    NOTIFY,
    /** Use fixed-delay polling only. */
    POLLING
}
