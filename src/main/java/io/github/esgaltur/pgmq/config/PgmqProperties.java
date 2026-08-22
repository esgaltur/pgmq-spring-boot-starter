package io.github.esgaltur.pgmq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "spring.pgmq")
public class PgmqProperties {

    public enum ListenerMode {
        /**
         * Wait for PostgreSQL notifications and periodically scan as a recovery mechanism.
         */
        NOTIFY,
        /**
         * Poll every listener according to its {@code pollInterval} setting.
         */
        POLLING
    }

    /**
     * How listener workers wait for new messages.
     */
    private ListenerMode listenerMode = ListenerMode.NOTIFY;

    /**
     * Maximum idle time between recovery scans in NOTIFY mode. Recovery scans
     * cover notifications missed during disconnects and messages becoming visible
     * after a delay or visibility timeout.
     */
    private Duration notificationRecoveryInterval = Duration.ofSeconds(30);

    /**
     * Minimum interval between notifications emitted by a queue. Workers perform
     * one confirmation scan at the end of this interval so throttled inserts are
     * not left waiting for the recovery scan.
     */
    private Duration notificationThrottleInterval = Duration.ofMillis(250);

    /**
     * Delay before reconnecting a failed LISTEN connection.
     */
    private Duration notificationReconnectInterval = Duration.ofSeconds(1);

    /**
     * Automatically call pgmq.enable_notify_insert for listener queues.
     * Disable this when notification triggers are managed by database migrations.
     */
    private boolean autoEnableNotifications = true;

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
    private Duration shutdownTimeout = Duration.ofSeconds(10);
}
