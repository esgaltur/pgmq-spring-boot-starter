package io.github.esgaltur.pgmq.core;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Default JDBC implementation of {@link PgmqIdempotencyRepository} that stores
 * processed message IDs in a dedicated table within the same database as PGMQ.
 */
public class JdbcPgmqIdempotencyRepository implements PgmqIdempotencyRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcPgmqIdempotencyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean isProcessed(String queueName, long msgId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_idempotency WHERE queue_name = ? AND msg_id = ?",
                Integer.class, queueName, msgId);
        return count != null && count > 0;
    }

    @Override
    public void markProcessed(String queueName, long msgId) {
        jdbcTemplate.update(
                "INSERT INTO pgmq_idempotency (queue_name, msg_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                queueName, msgId);
    }
}
