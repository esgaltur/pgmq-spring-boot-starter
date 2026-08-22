package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListenerMode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Fixed-delay wake-up strategy retained for compatibility and comparison. */
public final class PgmqPollingListenerWakeupStrategy implements PgmqListenerWakeupStrategy {

    private final List<PgmqQueueSignal> workerSignals = new ArrayList<>();

    @Override
    public void start(Map<String, PgmqListenerMode> queues) {
        // Polling needs no shared resource.
    }

    @Override
    public WaitHandle createWaitHandle(String queue, Duration pollingInterval) {
        PgmqQueueSignal signal = new PgmqQueueSignal();
        workerSignals.add(signal);
        return signal.waitHandle(
                pollingInterval,
                WakeupReason.POLLING,
                Duration.ZERO,
                Duration.ZERO);
    }

    @Override
    public String description() {
        return "POLLING";
    }

    @Override
    public void close() {
        workerSignals.forEach(PgmqQueueSignal::signal);
        workerSignals.clear();
    }
}
