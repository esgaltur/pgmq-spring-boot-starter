package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListenerMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PgmqListenerStatusTest {

    @Test
    void reportsDegradedHealthWhileNotificationQueuesRecover() {
        PgmqListenerStatus status = new PgmqListenerStatus();
        status.queueConfigured("orders", PgmqListenerMode.NOTIFY, PgmqListenerMode.NOTIFY, "supported");
        status.processorStarted();
        status.disconnected();

        PgmqListenerHealthIndicator indicator = new PgmqListenerHealthIndicator(status);

        assertEquals("DEGRADED", indicator.health().getStatus().getCode());
        status.connected(false);
        assertEquals("UP", indicator.health().getStatus().getCode());
    }

    @Test
    void exposesCumulativeWakeupAndReadCounters() {
        PgmqListenerStatus status = new PgmqListenerStatus();
        status.notificationReceived();
        status.wakeup(PgmqListenerWakeupStrategy.WakeupReason.RECOVERY);
        status.wakeup(PgmqListenerWakeupStrategy.WakeupReason.SCHEDULED);
        status.notificationWakeupSuppressed();
        status.queueRead();
        status.emptyRead();

        PgmqListenerStatus.Snapshot snapshot = status.snapshot();
        assertEquals(1L, snapshot.notifications());
        assertEquals(1L, snapshot.recoveryScans());
        assertEquals(1L, snapshot.scheduledWakeups());
        assertEquals(1L, snapshot.suppressedWakeups());
        assertEquals(1L, snapshot.queueReads());
        assertEquals(1L, snapshot.emptyReads());
    }
}
