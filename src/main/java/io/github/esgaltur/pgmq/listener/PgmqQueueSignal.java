package io.github.esgaltur.pgmq.listener;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class PgmqQueueSignal {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private long generation;
    private long lastSignalNanos;

    long generation() {
        lock.lock();
        try {
            return generation;
        } finally {
            lock.unlock();
        }
    }

    void signal() {
        lock.lock();
        try {
            generation++;
            lastSignalNanos = System.nanoTime();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    PgmqListenerWakeupStrategy.WakeupReason awaitChange(
            long observedGeneration,
            Duration baseTimeout,
            PgmqListenerWakeupStrategy.WakeupReason baseReason,
            Duration notificationQuietPeriod,
            Duration notificationJitter,
            Optional<Duration> nextVisibleDelay) throws InterruptedException {
        long baseNanos = positiveNanos(baseTimeout);
        long remainingNanos = baseNanos;
        PgmqListenerWakeupStrategy.WakeupReason timeoutReason = baseReason;
        boolean notificationReceived;
        lock.lockInterruptibly();
        try {
            long quietPeriodRemaining = notificationQuietPeriod.toNanos()
                    - (System.nanoTime() - lastSignalNanos);
            if (generation == observedGeneration && lastSignalNanos != 0L && quietPeriodRemaining > 0L) {
                remainingNanos = Math.min(remainingNanos, quietPeriodRemaining);
                if (remainingNanos == quietPeriodRemaining) {
                    timeoutReason = PgmqListenerWakeupStrategy.WakeupReason.CONFIRMATION;
                }
            }
            if (nextVisibleDelay.isPresent()) {
                long scheduledNanos = positiveNanos(nextVisibleDelay.get());
                if (scheduledNanos < remainingNanos) {
                    remainingNanos = scheduledNanos;
                    timeoutReason = PgmqListenerWakeupStrategy.WakeupReason.SCHEDULED;
                }
            }
            while (generation == observedGeneration && remainingNanos > 0L) {
                remainingNanos = changed.awaitNanos(remainingNanos);
            }
            notificationReceived = generation != observedGeneration;
        } finally {
            lock.unlock();
        }

        if (notificationReceived) {
            applyJitter(notificationJitter);
            return PgmqListenerWakeupStrategy.WakeupReason.NOTIFICATION;
        }
        return timeoutReason;
    }

    PgmqListenerWakeupStrategy.WaitHandle waitHandle(
            Duration baseTimeout,
            PgmqListenerWakeupStrategy.WakeupReason baseReason,
            Duration notificationQuietPeriod,
            Duration notificationJitter) {
        return new PgmqListenerWakeupStrategy.WaitHandle() {
            @Override
            public long snapshot() {
                return generation();
            }

            @Override
            public PgmqListenerWakeupStrategy.WakeupReason awaitChange(
                    long observedGeneration,
                    Optional<Duration> nextVisibleDelay) throws InterruptedException {
                return PgmqQueueSignal.this.awaitChange(
                        observedGeneration,
                        baseTimeout,
                        baseReason,
                        notificationQuietPeriod,
                        notificationJitter,
                        nextVisibleDelay);
            }
        };
    }

    private static long positiveNanos(Duration duration) {
        if (duration.isZero() || duration.isNegative()) {
            return 1L;
        }
        try {
            return duration.toNanos();
        } catch (ArithmeticException durationOverflow) {
            return Long.MAX_VALUE;
        }
    }

    private static void applyJitter(Duration jitter) throws InterruptedException {
        long upperBoundMillis = jitter.toMillis();
        if (upperBoundMillis <= 0L) {
            return;
        }
        long randomBound = upperBoundMillis == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : upperBoundMillis + 1L;
        long delayMillis = ThreadLocalRandom.current().nextLong(randomBound);
        TimeUnit.MILLISECONDS.sleep(delayMillis);
    }
}
