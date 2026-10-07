package io.github.esgaltur.pgmq.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Component
public @interface PgmqListener {
    /**
     * The name of the queue to listen to. Supports Spring property placeholders
     * such as {@code ${my.queue}}.
     */
    String queue();

    /**
     * Overrides the application-wide listener wake-up mode for this queue.
     */
    PgmqListenerMode mode() default PgmqListenerMode.DEFAULT;

    /**
     * Visibility timeout in seconds.
     */
    int vt() default 30;

    /**
     * How many messages to fetch at once.
     */
    int qty() default 1;

    /**
     * Poll interval in milliseconds.
     */
    long pollInterval() default 1000L;

    /**
     * If true, the message will be archived after successful processing.
     * If false, the message will be deleted.
     */
    boolean archive() default true;

    /**
     * If true, completed message IDs are recorded and skipped on later redelivery.
     * This does not guarantee exactly-once external side effects.
     */
    boolean idempotent() default false;

    /**
     * If true (default), the listener method, the idempotency record and finalizing the message run in
     * one database transaction, so the listener's own database work commits only with the message.
     * Set false for long-running handlers (for example CPU-bound work taking seconds): the method then
     * runs without holding a connection or a transaction, manages its own short transactions, and the
     * message is finalized only after the method returns normally. A failure still leaves the message
     * in the queue for the usual retry, backoff and dead-letter handling.
     */
    boolean transactional() default true;

    /**
     * Maximum number of times a message can be read before being considered a poison pill.
     * Set to 0 or negative to disable (default).
     */
    int maxRetries() default -1;

    /**
     * The name of the Dead Letter Queue to route poison pills to. Supports
     * Spring property placeholders.
     * If empty, poison pills are simply archived/deleted based on the archive() flag.
     */
    String deadLetterQueue() default "";

    /**
     * Number of concurrent threads to spin up for this listener. Supports
     * Spring property placeholders.
     * Useful for scaling out throughput on a single queue.
     */
    String concurrency() default "1";

    /**
     * Multiplier for exponential backoff on retries.
     * e.g., if vt=30s and multiplier=2.0: Retry 1 = 60s, Retry 2 = 120s.
     * Set to 1.0 (default) to disable exponential backoff (constant delay).
     */
    double backoffMultiplier() default 1.0;

    /**
     * Maximum visibility timeout in seconds when using exponential backoff.
     * Default is 3600 seconds (1 hour).
     */
    int maxBackoff() default 3600;
}
