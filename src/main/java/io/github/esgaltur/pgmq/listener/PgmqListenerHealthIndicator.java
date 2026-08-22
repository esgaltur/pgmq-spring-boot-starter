package io.github.esgaltur.pgmq.listener;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/** Actuator health contribution backed by {@link PgmqListenerStatus}. */
public final class PgmqListenerHealthIndicator implements HealthIndicator {

    private final PgmqListenerStatus listenerStatus;

    public PgmqListenerHealthIndicator(PgmqListenerStatus listenerStatus) {
        this.listenerStatus = listenerStatus;
    }

    @Override
    public Health health() {
        PgmqListenerStatus.Snapshot snapshot = listenerStatus.snapshot();
        boolean notificationQueues = snapshot.queues().values().stream()
                .anyMatch(queue -> queue.effectiveMode() == io.github.esgaltur.pgmq.annotation.PgmqListenerMode.NOTIFY);

        Health.Builder builder;
        if (!snapshot.running() && !snapshot.queues().isEmpty()) {
            builder = Health.down();
        } else if (notificationQueues
                && snapshot.connectionState() != PgmqListenerStatus.ConnectionState.CONNECTED) {
            builder = Health.status("DEGRADED");
        } else {
            builder = Health.up();
        }

        return builder
                .withDetail("running", snapshot.running())
                .withDetail("listenConnection", snapshot.connectionState())
                .withDetail("queues", snapshot.queues())
                .withDetail("notifications", snapshot.notifications())
                .withDetail("reconnects", snapshot.reconnects())
                .withDetail("recoveryScans", snapshot.recoveryScans())
                .withDetail("emptyReads", snapshot.emptyReads())
                .build();
    }
}
