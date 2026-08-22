package io.github.esgaltur.pgmq.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.core.JdbcPgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.listener.PgmqListenerProcessor;
import io.github.esgaltur.pgmq.listener.PgmqListenerRegistrar;
import io.github.esgaltur.pgmq.listener.PgmqListenerStatus;
import io.github.esgaltur.pgmq.listener.PgmqListenerMetrics;
import io.github.esgaltur.pgmq.listener.PgmqMessageHandler;
import io.github.esgaltur.pgmq.listener.PgmqListenerWakeupStrategy;
import io.github.esgaltur.pgmq.listener.PgmqNotifyListenerWakeupStrategy;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.annotation.Role;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;
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
    public PgmqTemplate pgmqTemplate(JdbcTemplate jdbcTemplate, ObjectProvider<ObjectMapper> objectMapperProvider) {
        // Spring Boot 4 auto-configures a Jackson 3 ObjectMapper, so a Jackson 2
        // com.fasterxml.jackson.databind.ObjectMapper bean may be absent. Fall back to a
        // bundled Jackson 2 instance so PGMQ works regardless of the app's Jackson version.
        ObjectMapper objectMapper = objectMapperProvider.getIfAvailable(() -> new ObjectMapper().findAndRegisterModules());
        return new PgmqTemplate(jdbcTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqIdempotencyRepository pgmqIdempotencyRepository(JdbcTemplate jdbcTemplate) {
        return new JdbcPgmqIdempotencyRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqListenerStatus pgmqListenerStatus() {
        return new PgmqListenerStatus();
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqMessageHandler pgmqMessageHandler(
            PgmqTemplate pgmqTemplate,
            PgmqIdempotencyRepository idempotencyRepository,
            ObjectProvider<PlatformTransactionManager> transactionManagerProvider) {
        PlatformTransactionManager transactionManager = transactionManagerProvider.getIfAvailable();
        TransactionOperations transactions = transactionManager == null
                ? TransactionOperations.withoutTransaction()
                : new TransactionTemplate(transactionManager);
        return new PgmqMessageHandler(pgmqTemplate, idempotencyRepository, transactions);
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqListenerWakeupStrategy pgmqListenerWakeupStrategy(
            PgmqTemplate pgmqTemplate,
            DataSource dataSource,
            @PgmqNotificationDataSource ObjectProvider<DataSource> notificationDataSourceProvider,
            PgmqListenerStatus listenerStatus) {
        DataSource notificationDataSource = notificationDataSourceProvider.getIfAvailable(() -> dataSource);
        return new PgmqNotifyListenerWakeupStrategy(
                pgmqTemplate, notificationDataSource, pgmqProperties, listenerStatus);
    }

    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public static PgmqListenerRegistrar pgmqListenerRegistrar() {
        return new PgmqListenerRegistrar();
    }

    @Bean
    @ConditionalOnMissingBean
    public PgmqListenerProcessor pgmqListenerProcessor(
            PgmqListenerRegistrar listenerRegistrar,
            PgmqTemplate pgmqTemplate, 
            PgmqMessageHandler messageHandler,
            PgmqListenerWakeupStrategy wakeupStrategy,
            PgmqListenerStatus listenerStatus,
            ObjectProvider<PgmqListenerMetrics> listenerMetricsProvider) {
        return new PgmqListenerProcessor(
                listenerRegistrar, pgmqTemplate, messageHandler, pgmqProperties,
                wakeupStrategy, listenerStatus,
                listenerMetricsProvider.getIfAvailable(() -> PgmqListenerMetrics.NO_OP));
    }
}
