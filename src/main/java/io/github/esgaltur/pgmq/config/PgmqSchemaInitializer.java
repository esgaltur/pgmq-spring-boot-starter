package io.github.esgaltur.pgmq.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

/**
 * Creates the PGMQ extension and the starter's own tables at startup
 * ({@code spring.pgmq.initialize-schema=always}, the default).
 *
 * <p>Every statement is idempotent ({@code IF NOT EXISTS}), so the script runs as a whole and any
 * failure stops the application with an explanation. It used to continue on errors, which let an
 * application start without PGMQ and fail later, at its first queue operation.</p>
 */
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
            new ResourceDatabasePopulator(new ClassPathResource("schema-pgmq.sql")).execute(dataSource);
        } catch (RuntimeException e) {
            throw new IllegalStateException("""
                    PGMQ schema initialization failed. The pgmq extension must be available to this \
                    PostgreSQL server (its files installed, see https://github.com/pgmq/pgmq) and the \
                    connecting role must be allowed to create it. Alternatively set \
                    spring.pgmq.initialize-schema=never and create the extension and the starter's \
                    tables in your own migrations (see schema-pgmq.sql).""", e);
        }
        log.info("PGMQ schema initialization complete.");
    }
}
