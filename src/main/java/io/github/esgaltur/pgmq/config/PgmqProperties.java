package io.github.esgaltur.pgmq.config;

import lombok.Data;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "spring.pgmq")
public class PgmqProperties implements InitializingBean {

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
    private ListenerMode listenerMode = ListenerMode.POLLING;

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
     * Maximum random delay applied after a notification before a consumer reads.
     * This spreads competing reads across application replicas. Set to zero to
     * prioritize the lowest possible wake-up latency.
     */
    private Duration notificationWakeupJitter = Duration.ofMillis(25);

    /**
     * Coordinate notification wake-ups through a short database lease so only
     * one application instance immediately reads a broadcast queue notification.
     */
    private boolean coordinateNotificationWakeups = true;

    /**
     * Duration of the cross-instance notification wake-up lease.
     */
    private Duration notificationWakeupLease = Duration.ofSeconds(1);

    /**
     * Inspect PGMQ visibility timestamps after an empty read and wake near the
     * next delayed or retried message instead of waiting for the recovery scan.
     */
    private boolean scheduleDelayedMessages = true;

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

    @Override
    public void afterPropertiesSet() {
        requirePositive(notificationRecoveryInterval, "notification-recovery-interval");
        requireNonNegative(notificationThrottleInterval, "notification-throttle-interval");
        requirePositive(notificationReconnectInterval, "notification-reconnect-interval");
        requireNonNegative(notificationWakeupJitter, "notification-wakeup-jitter");
        requirePositive(notificationWakeupLease, "notification-wakeup-lease");
        requireNonNegative(shutdownTimeout, "shutdown-timeout");
        if (notificationWakeupJitter.compareTo(notificationRecoveryInterval) >= 0) {
            throw new IllegalArgumentException(
                    "spring.pgmq.notification-wakeup-jitter must be shorter than "
                            + "spring.pgmq.notification-recovery-interval");
        }
        if (notificationWakeupLease.compareTo(notificationRecoveryInterval) >= 0) {
            throw new IllegalArgumentException(
                    "spring.pgmq.notification-wakeup-lease must be shorter than "
                            + "spring.pgmq.notification-recovery-interval");
        }
        if (notificationThrottleInterval.toMillis() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("spring.pgmq.notification-throttle-interval is too large");
        }
        if (defaultVt <= 0) {
            throw new IllegalArgumentException("spring.pgmq.default-vt must be greater than zero");
        }
        if (defaultPollInterval <= 0) {
            throw new IllegalArgumentException("spring.pgmq.default-poll-interval must be greater than zero");
        }
        if (defaultQty <= 0) {
            throw new IllegalArgumentException("spring.pgmq.default-qty must be greater than zero");
        }
        if (shutdownTimeout.getSeconds() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("spring.pgmq.shutdown-timeout is too large");
        }
    }

    private static void requirePositive(Duration value, String property) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("spring.pgmq." + property + " must be greater than zero");
        }
    }

    private static void requireNonNegative(Duration value, String property) {
        if (value == null || value.isNegative()) {
            throw new IllegalArgumentException("spring.pgmq." + property + " must not be negative");
        }
    }
}
