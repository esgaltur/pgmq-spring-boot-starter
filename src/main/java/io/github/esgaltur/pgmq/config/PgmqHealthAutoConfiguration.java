package io.github.esgaltur.pgmq.config;

import io.github.esgaltur.pgmq.listener.PgmqListenerHealthIndicator;
import io.github.esgaltur.pgmq.listener.PgmqListenerStatus;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

/** Isolates the optional Spring Boot health integration from the core starter. */
@AutoConfiguration(after = PgmqAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
public class PgmqHealthAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "pgmqListenerHealthIndicator")
    public PgmqListenerHealthIndicator pgmqListenerHealthIndicator(PgmqListenerStatus listenerStatus) {
        return new PgmqListenerHealthIndicator(listenerStatus);
    }
}
