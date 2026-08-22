package io.github.esgaltur.pgmq.listener;

import java.time.Duration;
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

    void awaitChange(
            long observedGeneration,
            Duration recoveryTimeout,
            Duration notificationQuietPeriod) throws InterruptedException {
        long remainingNanos = recoveryTimeout.toNanos();
        lock.lockInterruptibly();
        try {
            long quietPeriodRemaining = notificationQuietPeriod.toNanos()
                    - (System.nanoTime() - lastSignalNanos);
            if (generation == observedGeneration && lastSignalNanos != 0L && quietPeriodRemaining > 0L) {
                remainingNanos = Math.min(remainingNanos, quietPeriodRemaining);
            }
            while (generation == observedGeneration && remainingNanos > 0L) {
                remainingNanos = changed.awaitNanos(remainingNanos);
            }
        } finally {
            lock.unlock();
        }
    }

    PgmqListenerWakeupStrategy.WaitHandle waitHandle(
            Duration recoveryTimeout,
            Duration notificationQuietPeriod) {
        return new PgmqListenerWakeupStrategy.WaitHandle() {
            @Override
            public long snapshot() {
                return generation();
            }

            @Override
            public void awaitChange(long observedGeneration) throws InterruptedException {
                PgmqQueueSignal.this.awaitChange(
                        observedGeneration, recoveryTimeout, notificationQuietPeriod);
            }
        };
    }
}
