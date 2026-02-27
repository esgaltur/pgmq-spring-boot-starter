package io.github.esgaltur.pgmq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "spring.pgmq")
public class PgmqProperties {

    /**
     * Default visibility timeout in seconds.
     */
    private int defaultVt = 30;

    /**
     * Default poll interval in milliseconds.
     */
    private long defaultPollInterval = 1000L;

    /**
     * Default quantity of messages per poll.
     */
    private int defaultQty = 1;

    /**
     * Whether to automatically create the queue if it does not exist.
     */
    private boolean autoCreateQueue = false;

    public enum SchemaInitializationMode {
        /**
         * Always initialize the schema.
         */
        ALWAYS,
        /**
         * Never initialize the schema. (Use this in production with Flyway/Liquibase).
         */
        NEVER
    }

    /**
     * Whether to automatically initialize the PGMQ schema (Extension and Idempotency table).
     */
    private SchemaInitializationMode initializeSchema = SchemaInitializationMode.ALWAYS;

    /**
     * Whether to archive messages after successful processing by default.
     */
    private boolean defaultArchive = true;

    /**
     * How long to wait for in-flight messages to finish processing during application shutdown.
     */
    private java.time.Duration shutdownTimeout = java.time.Duration.ofSeconds(10);
}
