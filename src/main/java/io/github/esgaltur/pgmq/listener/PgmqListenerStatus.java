package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListenerMode;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe operational state and cumulative counters for PGMQ listeners.
 * Applications may inject this bean even when Spring Boot Actuator is absent.
 */
public final class PgmqListenerStatus {

    public enum ConnectionState {
        DISABLED,
        CONNECTING,
        CONNECTED,
        DISCONNECTED,
        STOPPED
    }

    public record QueueStatus(
            PgmqListenerMode requestedMode,
            PgmqListenerMode effectiveMode,
            String detail) {
    }

    public record Snapshot(
            boolean running,
            ConnectionState connectionState,
            Map<String, QueueStatus> queues,
            long notifications,
            long reconnects,
            long recoveryScans,
            long scheduledWakeups,
            long pollingWakeups,
            long suppressedWakeups,
            long queueReads,
            long emptyReads,
            long pollFailures,
            Instant lastConnectedAt,
            Instant lastDisconnectedAt) {
    }

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<ConnectionState> connectionState =
            new AtomicReference<>(ConnectionState.DISABLED);
    private final Map<String, QueueStatus> queues = new ConcurrentHashMap<>();
    private final AtomicLong notifications = new AtomicLong();
    private final AtomicLong reconnects = new AtomicLong();
    private final AtomicLong recoveryScans = new AtomicLong();
    private final AtomicLong scheduledWakeups = new AtomicLong();
    private final AtomicLong pollingWakeups = new AtomicLong();
    private final AtomicLong suppressedWakeups = new AtomicLong();
    private final AtomicLong queueReads = new AtomicLong();
    private final AtomicLong emptyReads = new AtomicLong();
    private final AtomicLong pollFailures = new AtomicLong();
    private final AtomicReference<Instant> lastConnectedAt = new AtomicReference<>();
    private final AtomicReference<Instant> lastDisconnectedAt = new AtomicReference<>();

    public Snapshot snapshot() {
        return new Snapshot(
                running.get(), connectionState.get(), Map.copyOf(queues),
                notifications.get(), reconnects.get(), recoveryScans.get(),
                scheduledWakeups.get(), pollingWakeups.get(), suppressedWakeups.get(),
                queueReads.get(), emptyReads.get(),
                pollFailures.get(), lastConnectedAt.get(), lastDisconnectedAt.get());
    }

    void processorStarted() {
        running.set(true);
    }

    void processorStopped() {
        running.set(false);
    }

    void queueConfigured(
            String queue,
            PgmqListenerMode requested,
            PgmqListenerMode effective,
            String detail) {
        queues.put(queue, new QueueStatus(requested, effective, detail));
    }

    void clearQueues() {
        queues.clear();
    }

    void notificationsDisabled() {
        connectionState.set(ConnectionState.DISABLED);
    }

    void connecting() {
        connectionState.set(ConnectionState.CONNECTING);
    }

    void connected(boolean reconnect) {
        connectionState.set(ConnectionState.CONNECTED);
        lastConnectedAt.set(Instant.now());
        if (reconnect) {
            reconnects.incrementAndGet();
        }
    }

    void disconnected() {
        connectionState.set(ConnectionState.DISCONNECTED);
        lastDisconnectedAt.set(Instant.now());
    }

    void connectionStopped() {
        connectionState.set(ConnectionState.STOPPED);
    }

    void notificationReceived() {
        notifications.incrementAndGet();
    }

    void wakeup(PgmqListenerWakeupStrategy.WakeupReason reason) {
        switch (reason) {
            case RECOVERY -> recoveryScans.incrementAndGet();
            case SCHEDULED -> scheduledWakeups.incrementAndGet();
            case POLLING -> pollingWakeups.incrementAndGet();
            case NOTIFICATION, CONFIRMATION -> {
                // Notification receipt is counted by the coordinator. Confirmation
                // wake-ups are a consequence of the same notification burst.
            }
        }
    }

    void emptyRead() {
        emptyReads.incrementAndGet();
    }

    void notificationWakeupSuppressed() {
        suppressedWakeups.incrementAndGet();
    }

    void queueRead() {
        queueReads.incrementAndGet();
    }

    void pollFailure() {
        pollFailures.incrementAndGet();
    }
}
