package io.github.esgaltur.pgmq.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

@Slf4j
@RequiredArgsConstructor
public class PgmqSchemaInitializer implements InitializingBean {

    private final DataSource dataSource;
    private final PgmqProperties properties;

    @Override
    public void afterPropertiesSet() {
        if (properties.getInitializeSchema() == PgmqProperties.SchemaInitializationMode.NEVER) {
            log.info("PGMQ schema initialization is disabled (spring.pgmq.initialize-schema=never).");
            return;
        }

        log.info("Initializing PGMQ schema from schema-pgmq.sql...");
        try {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            populator.addScript(new ClassPathResource("schema-pgmq.sql"));
            populator.setContinueOnError(true); // Don't crash if extension already exists
            populator.execute(dataSource);
            log.info("PGMQ schema initialization complete.");
        } catch (Exception e) {
            log.error("Failed to initialize PGMQ schema: {}", e.getMessage());
        }
    }
}
