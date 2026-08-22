package io.github.esgaltur.pgmq.config;

import io.github.esgaltur.pgmq.listener.PgmqListenerMetrics;
import io.github.esgaltur.pgmq.listener.PgmqListenerStatus;
import io.github.esgaltur.pgmq.listener.PgmqMicrometerListenerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/** Isolates the optional Micrometer adapter from the core listener runtime. */
@AutoConfiguration(after = PgmqAutoConfiguration.class)
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnBean(MeterRegistry.class)
public class PgmqMetricsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(PgmqListenerMetrics.class)
    public PgmqListenerMetrics pgmqListenerMetrics(
            MeterRegistry meterRegistry,
            PgmqListenerStatus listenerStatus) {
        return new PgmqMicrometerListenerMetrics(meterRegistry, listenerStatus);
    }
}
