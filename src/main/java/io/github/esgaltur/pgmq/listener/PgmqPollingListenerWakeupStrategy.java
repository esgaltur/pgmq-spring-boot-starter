package io.github.esgaltur.pgmq.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Fixed-delay wake-up strategy retained for compatibility and comparison. */
public final class PgmqPollingListenerWakeupStrategy implements PgmqListenerWakeupStrategy {

    private final List<PgmqQueueSignal> workerSignals = new ArrayList<>();

    @Override
    public void start(Set<String> queues) {
        // Polling needs no shared resource.
    }

    @Override
    public WaitHandle createWaitHandle(String queue, Duration pollingInterval) {
        PgmqQueueSignal signal = new PgmqQueueSignal();
        workerSignals.add(signal);
        return signal.waitHandle(pollingInterval, Duration.ZERO);
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
