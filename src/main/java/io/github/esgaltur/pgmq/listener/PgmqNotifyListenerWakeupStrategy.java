package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListenerMode;
import io.github.esgaltur.pgmq.config.PgmqProperties;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * PostgreSQL LISTEN/NOTIFY strategy with a polling strategy as its per-queue and
 * connection-level fallback.
 */
@Slf4j
public final class PgmqNotifyListenerWakeupStrategy implements PgmqListenerWakeupStrategy {

    private final PgmqTemplate pgmqTemplate;
    private final DataSource dataSource;
    private final PgmqProperties properties;
    private final PgmqListenerStatus listenerStatus;
    private final PgmqPollingListenerWakeupStrategy fallback = new PgmqPollingListenerWakeupStrategy();
    private final Map<String, PgmqQueueSignal> notificationSignals = new HashMap<>();
    private final String instanceId = UUID.randomUUID().toString();

    private PgmqNotificationCoordinator coordinator;

    public PgmqNotifyListenerWakeupStrategy(
            PgmqTemplate pgmqTemplate,
            DataSource dataSource,
            PgmqProperties properties,
            PgmqListenerStatus listenerStatus) {
        this.pgmqTemplate = pgmqTemplate;
        this.dataSource = dataSource;
        this.properties = properties;
        this.listenerStatus = listenerStatus;
    }

    @Override
    public void start(Map<String, PgmqListenerMode> queues) {
        log.debug("Preparing LISTEN/NOTIFY wake-up strategy for {} queue(s).", queues.size());
        notificationSignals.clear();
        listenerStatus.clearQueues();
        coordinator = new PgmqNotificationCoordinator(
                dataSource,
                properties.getNotificationReconnectInterval(),
                listenerStatus);

        PgmqTemplate.NotificationCapability capability = detectCapability();

        for (Map.Entry<String, PgmqListenerMode> queueEntry : queues.entrySet()) {
            String queue = queueEntry.getKey();
            PgmqListenerMode requestedMode = queueEntry.getValue();
            if (requestedMode == PgmqListenerMode.POLLING) {
                listenerStatus.queueConfigured(queue, requestedMode, PgmqListenerMode.POLLING,
                        "Polling selected by configuration");
                continue;
            }
            if (!capability.supported()) {
                listenerStatus.queueConfigured(queue, requestedMode, PgmqListenerMode.POLLING,
                        capability.detail());
                log.warn("PGMQ notifications are unavailable for queue {}: {}. Using polling.",
                        queue, capability.detail());
                continue;
            }
            if (!enableNotifications(queue)) {
                listenerStatus.queueConfigured(queue, requestedMode, PgmqListenerMode.POLLING,
                        "Could not enable insert notifications");
                continue;
            }
            String channel = pgmqTemplate.getInsertNotificationChannel(queue);
            notificationSignals.put(queue, coordinator.register(channel));
            listenerStatus.queueConfigured(queue, requestedMode, PgmqListenerMode.NOTIFY,
                    capability.detail());
        }

        if (notificationSignals.isEmpty()) {
            listenerStatus.notificationsDisabled();
            coordinator = null;
            return;
        }

        try {
            coordinator.start();
        } catch (Exception e) {
            coordinator.close();
            coordinator = null;
            notificationSignals.clear();
            listenerStatus.notificationsDisabled();
            queues.forEach((queue, requestedMode) -> listenerStatus.queueConfigured(
                    queue, requestedMode, PgmqListenerMode.POLLING,
                    "LISTEN connection could not be started"));
            log.warn("Could not start the PGMQ LISTEN connection. Falling back to polling.", e);
        }
    }

    private PgmqTemplate.NotificationCapability detectCapability() {
        if (!properties.isAutoEnableNotifications()) {
            return new PgmqTemplate.NotificationCapability(
                    true, "externally-managed", "Notification trigger is managed externally");
        }
        try {
            return pgmqTemplate.getNotificationCapability();
        } catch (Exception exception) {
            log.warn("Could not detect PGMQ notification capabilities. Notification queues will use polling.");
            log.debug("PGMQ capability detection failure.", exception);
            return new PgmqTemplate.NotificationCapability(
                    false, "unknown", "Notification capability detection failed");
        }
    }

    private boolean enableNotifications(String queue) {
        if (!properties.isAutoEnableNotifications()) {
            log.debug("Automatic notification setup is disabled for queue {}; expecting a database-managed trigger.",
                    queue);
            return true;
        }
        try {
            int throttleIntervalMillis = Math.toIntExact(properties.getNotificationThrottleInterval().toMillis());
            pgmqTemplate.enableInsertNotifications(queue, throttleIntervalMillis);
            log.debug("Enabled insert notifications for queue {} with a {} ms throttle interval.",
                    queue, throttleIntervalMillis);
            return true;
        } catch (Exception e) {
            log.warn("Could not enable PGMQ insert notifications for {}. This queue will use polling.",
                    queue);
            log.debug("PGMQ notification setup failure for queue {}.", queue, e);
            return false;
        }
    }

    @Override
    public WaitHandle createWaitHandle(String queue, Duration pollingInterval) {
        PgmqQueueSignal signal = notificationSignals.get(queue);
        if (signal == null) {
            log.debug("Using polling wake-ups for queue {} with interval {}.", queue, pollingInterval);
            return fallback.createWaitHandle(queue, pollingInterval);
        }
        log.debug("Using LISTEN/NOTIFY wake-ups for queue {} with recovery interval {}.",
                queue, properties.getNotificationRecoveryInterval());
        WaitHandle waitHandle = signal.waitHandle(
                properties.getNotificationRecoveryInterval(),
                WakeupReason.RECOVERY,
                properties.getNotificationThrottleInterval(),
                properties.getNotificationWakeupJitter());
        return properties.isCoordinateNotificationWakeups()
                ? coordinateWakeups(queue, waitHandle)
                : waitHandle;
    }

    private WaitHandle coordinateWakeups(String queue, WaitHandle delegate) {
        return new WaitHandle() {
            @Override
            public long snapshot() {
                return delegate.snapshot();
            }

            @Override
            public WakeupReason awaitChange(
                    long observedGeneration,
                    Optional<Duration> nextVisibleDelay) throws InterruptedException {
                long currentGeneration = observedGeneration;
                Optional<Duration> scheduledDelay = nextVisibleDelay;
                while (true) {
                    WakeupReason reason = delegate.awaitChange(currentGeneration, scheduledDelay);
                    if (!requiresLease(reason) || claimWakeupLease(queue)) {
                        return reason;
                    }
                    listenerStatus.notificationWakeupSuppressed();
                    currentGeneration = delegate.snapshot();
                    scheduledDelay = Optional.empty();
                }
            }
        };
    }

    private boolean claimWakeupLease(String queue) {
        try {
            return pgmqTemplate.tryClaimNotificationWakeup(
                    queue, instanceId, properties.getNotificationWakeupLease());
        } catch (Exception exception) {
            log.warn("Could not coordinate the notification wake-up lease for queue {}; allowing the read.",
                    queue);
            log.debug("PGMQ wake-up lease failure for queue {}.", queue, exception);
            return true;
        }
    }

    private static boolean requiresLease(WakeupReason reason) {
        return reason == WakeupReason.NOTIFICATION
                || reason == WakeupReason.CONFIRMATION
                || reason == WakeupReason.SCHEDULED;
    }

    @Override
    public String description() {
        if (notificationSignals.isEmpty()) {
            return "POLLING";
        }
        boolean pollingQueues = listenerStatus.snapshot().queues().values().stream()
                .anyMatch(queue -> queue.effectiveMode() == PgmqListenerMode.POLLING);
        return pollingQueues ? "MIXED LISTEN/NOTIFY + POLLING" : "LISTEN/NOTIFY";
    }

    @Override
    public void close() {
        if (coordinator != null) {
            coordinator.close();
            coordinator = null;
        }
        fallback.close();
        notificationSignals.clear();
    }
}
