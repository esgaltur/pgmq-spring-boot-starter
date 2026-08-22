package io.github.esgaltur.pgmq.listener;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;
import java.util.function.LongSupplier;

/** Micrometer adapter for the core listener metrics port. */
public final class PgmqMicrometerListenerMetrics implements PgmqListenerMetrics {

    private final MeterRegistry meterRegistry;

    public PgmqMicrometerListenerMetrics(
            MeterRegistry meterRegistry,
            PgmqListenerStatus listenerStatus) {
        this.meterRegistry = meterRegistry;
        bindStatus(listenerStatus);
    }

    @Override
    public void registerQueue(String queue, LongSupplier depthSupplier) {
        Gauge.builder("pgmq.queue.depth", depthSupplier, LongSupplier::getAsLong)
                .tag("queue", queue)
                .description("Current number of messages in the PGMQ queue")
                .register(meterRegistry);
    }

    @Override
    public void messageProcessed(String queue, String status) {
        meterRegistry.counter("pgmq.messages.processed", "queue", queue, "status", status).increment();
    }

    @Override
    public void processingCompleted(String queue, String status, Duration duration) {
        meterRegistry.timer("pgmq.listener.latency", "queue", queue, "status", status)
                .record(duration);
    }

    private void bindStatus(PgmqListenerStatus status) {
        Gauge.builder("pgmq.listener.connected", status,
                        source -> source.snapshot().connectionState()
                                == PgmqListenerStatus.ConnectionState.CONNECTED ? 1.0 : 0.0)
                .description("Whether the dedicated PGMQ LISTEN connection is connected")
                .register(meterRegistry);
        bindCounter("pgmq.listener.notifications", status,
                source -> source.snapshot().notifications(),
                "PostgreSQL notifications received by PGMQ listeners");
        bindCounter("pgmq.listener.reconnects", status,
                source -> source.snapshot().reconnects(),
                "Successful PGMQ LISTEN reconnections");
        bindCounter("pgmq.listener.recovery.scans", status,
                source -> source.snapshot().recoveryScans(),
                "Listener reads caused by notification recovery timeouts");
        bindCounter("pgmq.listener.scheduled.wakeups", status,
                source -> source.snapshot().scheduledWakeups(),
                "Listener reads scheduled for delayed or retried messages");
        bindCounter("pgmq.listener.polling.wakeups", status,
                source -> source.snapshot().pollingWakeups(),
                "Listener reads caused by fixed-delay polling");
        bindCounter("pgmq.listener.suppressed.wakeups", status,
                source -> source.snapshot().suppressedWakeups(),
                "Immediate notification reads suppressed by a cross-instance lease");
        bindCounter("pgmq.listener.queue.reads", status,
                source -> source.snapshot().queueReads(),
                "Durable PGMQ read operations performed by listeners");
        bindCounter("pgmq.listener.empty.reads", status,
                source -> source.snapshot().emptyReads(),
                "PGMQ listener reads that returned no messages");
        bindCounter("pgmq.listener.poll.failures", status,
                source -> source.snapshot().pollFailures(),
                "Failed PGMQ listener read operations");
    }

    private void bindCounter(
            String name,
            PgmqListenerStatus status,
            java.util.function.ToDoubleFunction<PgmqListenerStatus> value,
            String description) {
        FunctionCounter.builder(name, status, value)
                .description(description)
                .register(meterRegistry);
    }
}
