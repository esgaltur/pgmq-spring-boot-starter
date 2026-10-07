package io.github.esgaltur.pgmq.config;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** On a stock PostgreSQL the extension is missing: startup must stop with an explanation. */
@Testcontainers
class PgmqSchemaInitializerTest {

    @Container
    static PostgreSQLContainer<?> stockPostgres = new PostgreSQLContainer<>("postgres:16-alpine");

    private DriverManagerDataSource dataSource() {
        return new DriverManagerDataSource(stockPostgres.getJdbcUrl(), stockPostgres.getUsername(), stockPostgres.getPassword());
    }

    @Test
    void aMissingExtensionStopsStartupWithAnExplanation() {
        PgmqProperties properties = new PgmqProperties();
        PgmqSchemaInitializer initializer = new PgmqSchemaInitializer(dataSource(), properties);

        IllegalStateException failure = assertThrows(IllegalStateException.class, initializer::afterPropertiesSet);
        assertTrue(failure.getMessage().contains("spring.pgmq.initialize-schema=never"), failure.getMessage());
    }

    @Test
    void neverLeavesTheSchemaToTheApplication() {
        PgmqProperties properties = new PgmqProperties();
        properties.setInitializeSchema(PgmqProperties.SchemaInitializationMode.NEVER);
        assertDoesNotThrow(new PgmqSchemaInitializer(dataSource(), properties)::afterPropertiesSet);
    }
}
