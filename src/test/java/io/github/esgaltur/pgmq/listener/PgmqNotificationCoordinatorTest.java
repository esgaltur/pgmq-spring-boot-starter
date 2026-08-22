package io.github.esgaltur.pgmq.listener;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PgmqNotificationCoordinatorTest {

    @Test
    void signalBeforeAwaitIsNotLost() throws InterruptedException {
        PgmqQueueSignal signal = new PgmqQueueSignal();
        long observedGeneration = signal.generation();

        signal.signal();

        long startedAt = System.nanoTime();
        signal.awaitChange(observedGeneration, Duration.ofSeconds(1), Duration.ZERO);
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertTrue(elapsedMillis < 100, "A signal arriving before await should be observed immediately");
    }

    @Test
    void awaitReturnsAfterTimeoutWhenNoSignalArrives() throws InterruptedException {
        PgmqQueueSignal signal = new PgmqQueueSignal();

        long startedAt = System.nanoTime();
        signal.awaitChange(signal.generation(), Duration.ofMillis(50), Duration.ZERO);
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertTrue(elapsedMillis >= 35, "Recovery wait should not busy-spin");
    }

    @Test
    void performsConfirmationWakeAtEndOfNotificationThrottleWindow() throws InterruptedException {
        PgmqQueueSignal signal = new PgmqQueueSignal();
        signal.signal();
        long observedGeneration = signal.generation();

        long startedAt = System.nanoTime();
        signal.awaitChange(observedGeneration, Duration.ofSeconds(1), Duration.ofMillis(50));
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertTrue(elapsedMillis >= 35 && elapsedMillis < 250,
                "A throttled notification should cause one confirmation scan after the quiet period");
    }
}
