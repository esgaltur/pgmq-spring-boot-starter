package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.config.PgmqProperties;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL LISTEN/NOTIFY strategy with a polling strategy as its per-queue and
 * connection-level fallback.
 */
@Slf4j
public final class PgmqNotifyListenerWakeupStrategy implements PgmqListenerWakeupStrategy {

    private final PgmqTemplate pgmqTemplate;
    private final DataSource dataSource;
    private final PgmqProperties properties;
    private final PgmqPollingListenerWakeupStrategy fallback = new PgmqPollingListenerWakeupStrategy();
    private final Map<String, PgmqQueueSignal> notificationSignals = new HashMap<>();

    private PgmqNotificationCoordinator coordinator;

    public PgmqNotifyListenerWakeupStrategy(
            PgmqTemplate pgmqTemplate,
            DataSource dataSource,
            PgmqProperties properties) {
        this.pgmqTemplate = pgmqTemplate;
        this.dataSource = dataSource;
        this.properties = properties;
    }

    @Override
    public void start(Set<String> queues) {
        log.debug("Preparing LISTEN/NOTIFY wake-up strategy for {} queue(s).", queues.size());
        notificationSignals.clear();
        coordinator = new PgmqNotificationCoordinator(dataSource, properties.getNotificationReconnectInterval());

        for (String queue : queues) {
            if (!enableNotifications(queue)) {
                continue;
            }
            String channel = pgmqTemplate.getInsertNotificationChannel(queue);
            notificationSignals.put(queue, coordinator.register(channel));
        }

        if (notificationSignals.isEmpty()) {
            if (!queues.isEmpty()) {
                log.warn("No PGMQ queue could use insert notifications; all listeners will use polling.");
            }
            return;
        }

        try {
            coordinator.start();
        } catch (Exception e) {
            coordinator.close();
            coordinator = null;
            notificationSignals.clear();
            log.warn("Could not start the PGMQ LISTEN connection. Falling back to polling.", e);
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
                    queue, e);
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
        return signal.waitHandle(
                properties.getNotificationRecoveryInterval(),
                properties.getNotificationThrottleInterval());
    }

    @Override
    public String description() {
        return notificationSignals.isEmpty() ? "POLLING (notification fallback)" : "LISTEN/NOTIFY";
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
