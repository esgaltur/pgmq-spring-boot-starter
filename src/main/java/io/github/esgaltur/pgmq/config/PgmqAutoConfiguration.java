package io.github.esgaltur.pgmq.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.core.JdbcPgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.listener.PgmqListenerProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;

@AutoConfiguration
@ConditionalOnClass(JdbcTemplate.class)
@EnableConfigurationProperties(PgmqProperties.class)
@ImportRuntimeHints(PgmqListenerRuntimeHints.class)
@RequiredArgsConstructor
public class PgmqAutoConfiguration {

    private final PgmqProperties pgmqProperties;

    @Bean
    @ConditionalOnMissingBean
    public PgmqSchemaInitializer pgmqSchemaInitializer(DataSource dataSource) {
        return new PgmqSchemaInitializer(dataSource, pgmqProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqTemplate pgmqTemplate(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        return new PgmqTemplate(jdbcTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqIdempotencyRepository pgmqIdempotencyRepository(JdbcTemplate jdbcTemplate) {
        return new JdbcPgmqIdempotencyRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqListenerProcessor pgmqListenerProcessor(
            PgmqTemplate pgmqTemplate, 
            PgmqIdempotencyRepository idempotencyRepository, 
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        return new PgmqListenerProcessor(pgmqTemplate, idempotencyRepository, pgmqProperties, meterRegistryProvider.getIfAvailable());
    }
}
