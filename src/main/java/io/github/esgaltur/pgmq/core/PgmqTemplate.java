package io.github.esgaltur.pgmq.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class PgmqTemplate {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new queue.
     * @param queueName The name of the queue.
     */
    public void createQueue(String queueName) {
        jdbcTemplate.execute("SELECT pgmq.create('" + queueName + "')");
    }

    /**
     * Sends a message to a queue for immediate delivery.
     * @param queueName The name of the queue.
     * @param payload The message payload.
     * @return The message ID.
     */
    public long send(String queueName, Object payload) {
        return sendWithDelay(queueName, payload, 0);
    }

    /**
     * Sends a delayed message to a queue. The message will remain invisible for the specified delay.
     * @param queueName The name of the queue.
     * @param payload The message payload.
     * @param delaySeconds The delay in seconds before the message becomes visible.
     * @return The message ID.
     */
    public long sendWithDelay(String queueName, Object payload, int delaySeconds) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            return jdbcTemplate.queryForObject(
                "SELECT pgmq.send(?, ?::jsonb, ?)",
                Long.class,
                queueName, jsonPayload, delaySeconds
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize payload", e);
        }
    }

    /**
     * Reads a list of messages from a queue.
     * @param queueName The name of the queue.
     * @param vt Visibility timeout in seconds.
     * @param qty Number of messages to read.
     * @param type The type of the payload.
     * @param <T> The payload type.
     * @return List of messages.
     */
    public <T> List<PgmqMessage<T>> read(String queueName, int vt, int qty, Class<T> type) {
        return jdbcTemplate.query(
            "SELECT * FROM pgmq.read(?, ?, ?)",
            new PgmqMessageRowMapper<>(objectMapper, type),
            queueName, vt, qty
        );
    }

    /**
     * Pops a single message from the queue (reads and deletes in one operation).
     * @param queueName The name of the queue.
     * @param type The type of the payload.
     * @param <T> The payload type.
     * @return Optional message.
     */
    public <T> Optional<PgmqMessage<T>> pop(String queueName, Class<T> type) {
        List<PgmqMessage<T>> results = jdbcTemplate.query(
            "SELECT * FROM pgmq.pop(?)",
            new PgmqMessageRowMapper<>(objectMapper, type),
            queueName
        );
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    /**
     * Archives a message.
     * @param queueName The name of the queue.
     * @param msgId The message ID.
     * @return true if archived.
     */
    public boolean archive(String queueName, long msgId) {
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(
            "SELECT pgmq.archive(?, ?)",
            Boolean.class,
            queueName, msgId
        ));
    }

    /**
     * Deletes a message.
     * @param queueName The name of the queue.
     * @param msgId The message ID.
     * @return true if deleted.
     */
    public boolean delete(String queueName, long msgId) {
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(
            "SELECT pgmq.delete(?, ?)",
            Boolean.class,
            queueName, msgId
        ));
    }

    /**
     * Updates the visibility timeout of a specific message.
     * @param queueName The name of the queue.
     * @param msgId The message ID.
     * @param vtSeconds The new visibility timeout in seconds.
     */
    public void setVt(String queueName, long msgId, int vtSeconds) {
        jdbcTemplate.queryForObject(
            "SELECT pgmq.set_vt(?, ?, ?)",
            Object.class,
            queueName, msgId, vtSeconds
        );
    }

    /**
     * Gets the current depth (length) of the queue.
     * @param queueName The name of the queue.
     * @return The number of messages currently in the queue.
     */
    public long getQueueDepth(String queueName) {
        try {
            Long length = jdbcTemplate.queryForObject(
                "SELECT queue_length FROM pgmq.metrics(?)",
                Long.class,
                queueName
            );
            return length != null ? length : 0L;
        } catch (Exception e) {
            log.warn("Failed to fetch queue depth for {}: {}", queueName, e.getMessage());
            return 0L;
        }
    }

    private record PgmqMessageRowMapper<T>(ObjectMapper objectMapper,
                                           Class<T> type) implements RowMapper<PgmqMessage<T>> {
            @Override
            public PgmqMessage<T> mapRow(ResultSet rs, int rowNum) throws SQLException {
                try {
                    String messageJson = rs.getString("message");
                    T payload = objectMapper.readValue(messageJson, type);

                    return PgmqMessage.<T>builder()
                            .msgId(rs.getLong("msg_id"))
                            .readCount(rs.getInt("read_ct"))
                            .enqueuedAt(rs.getObject("enqueued_at", OffsetDateTime.class))
                            .vt(rs.getObject("vt", OffsetDateTime.class))
                            .payload(payload)
                            .build();
                } catch (JsonProcessingException e) {
                    throw new SQLException("Failed to deserialize PGMQ message payload", e);
                }
            }
        }
}
