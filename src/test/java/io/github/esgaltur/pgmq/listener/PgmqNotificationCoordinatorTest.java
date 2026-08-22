package io.github.esgaltur.pgmq.listener;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PgmqNotificationCoordinatorTest {

    @Test
    void signalBeforeAwaitIsNotLost() throws InterruptedException {
        PgmqQueueSignal signal = new PgmqQueueSignal();
        long observedGeneration = signal.generation();

        signal.signal();

        long startedAt = System.nanoTime();
        PgmqListenerWakeupStrategy.WakeupReason reason = signal.awaitChange(
                observedGeneration,
                Duration.ofSeconds(1),
                PgmqListenerWakeupStrategy.WakeupReason.RECOVERY,
                Duration.ZERO,
                Duration.ZERO,
                Optional.empty());
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertTrue(elapsedMillis < 100, "A signal arriving before await should be observed immediately");
        org.junit.jupiter.api.Assertions.assertEquals(
                PgmqListenerWakeupStrategy.WakeupReason.NOTIFICATION, reason);
    }

    @Test
    void awaitReturnsAfterTimeoutWhenNoSignalArrives() throws InterruptedException {
        PgmqQueueSignal signal = new PgmqQueueSignal();

        long startedAt = System.nanoTime();
        PgmqListenerWakeupStrategy.WakeupReason reason = signal.awaitChange(
                signal.generation(),
                Duration.ofMillis(50),
                PgmqListenerWakeupStrategy.WakeupReason.RECOVERY,
                Duration.ZERO,
                Duration.ZERO,
                Optional.empty());
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertTrue(elapsedMillis >= 35, "Recovery wait should not busy-spin");
        org.junit.jupiter.api.Assertions.assertEquals(
                PgmqListenerWakeupStrategy.WakeupReason.RECOVERY, reason);
    }

    @Test
    void performsConfirmationWakeAtEndOfNotificationThrottleWindow() throws InterruptedException {
        PgmqQueueSignal signal = new PgmqQueueSignal();
        signal.signal();
        long observedGeneration = signal.generation();

        long startedAt = System.nanoTime();
        PgmqListenerWakeupStrategy.WakeupReason reason = signal.awaitChange(
                observedGeneration,
                Duration.ofSeconds(1),
                PgmqListenerWakeupStrategy.WakeupReason.RECOVERY,
                Duration.ofMillis(50),
                Duration.ZERO,
                Optional.empty());
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertTrue(elapsedMillis >= 35 && elapsedMillis < 250,
                "A throttled notification should cause one confirmation scan after the quiet period");
        org.junit.jupiter.api.Assertions.assertEquals(
                PgmqListenerWakeupStrategy.WakeupReason.CONFIRMATION, reason);
    }

    @Test
    void wakesAtNextMessageVisibilityBeforeRecoveryTimeout() throws InterruptedException {
        PgmqQueueSignal signal = new PgmqQueueSignal();

        long startedAt = System.nanoTime();
        PgmqListenerWakeupStrategy.WakeupReason reason = signal.awaitChange(
                signal.generation(),
                Duration.ofSeconds(5),
                PgmqListenerWakeupStrategy.WakeupReason.RECOVERY,
                Duration.ZERO,
                Duration.ZERO,
                Optional.of(Duration.ofMillis(50)));
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertTrue(elapsedMillis >= 35 && elapsedMillis < 250);
        org.junit.jupiter.api.Assertions.assertEquals(
                PgmqListenerWakeupStrategy.WakeupReason.SCHEDULED, reason);
    }
}
