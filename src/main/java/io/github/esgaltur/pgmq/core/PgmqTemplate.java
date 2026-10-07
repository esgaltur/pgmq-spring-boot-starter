package io.github.esgaltur.pgmq.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class PgmqTemplate {

    /** Database support detected for PGMQ insert notifications. */
    public record NotificationCapability(boolean supported, String pgmqVersion, String detail) {
    }

    private final JdbcTemplate jdbcTemplate;
    private final PgmqPayloadCodec payloadCodec;

    /** Convenience for Jackson 2 users; auto-configuration passes a {@link PgmqPayloadCodec}. */
    public PgmqTemplate(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this(jdbcTemplate, new Jackson2PayloadCodec(objectMapper));
    }

    /**
     * Creates a new queue.
     * @param queueName The name of the queue.
     */
    public void createQueue(String queueName) {
        executeQueueCommand("SELECT pgmq.create(?)", queueName);
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
        String jsonPayload = payloadCodec.write(payload);
        return jdbcTemplate.queryForObject(
            "SELECT pgmq.send(?, ?::jsonb, ?)",
            Long.class,
            queueName, jsonPayload, delaySeconds
        );
    }

    /**
     * Enables PGMQ's throttled insert notifications for a queue. The function is
     * idempotent and creates the PostgreSQL trigger used by LISTEN/NOTIFY consumers.
     *
     * @param queueName The queue whose inserts should emit notifications.
     */
    public void enableInsertNotifications(String queueName) {
        enableInsertNotifications(queueName, 250);
    }

    /**
     * Enables PGMQ insert notifications with an explicit throttle interval.
     *
     * @param queueName The queue whose inserts should emit notifications.
     * @param throttleIntervalMillis Minimum milliseconds between notifications.
     */
    public void enableInsertNotifications(String queueName, int throttleIntervalMillis) {
        jdbcTemplate.execute(
            "SELECT pgmq.enable_notify_insert(?, ?)",
            (PreparedStatementCallback<Void>) preparedStatement -> {
                preparedStatement.setString(1, queueName);
                preparedStatement.setInt(2, throttleIntervalMillis);
                preparedStatement.execute();
                return null;
            }
        );
    }

    /**
     * Returns the documented PostgreSQL channel used by PGMQ insert notifications.
     */
    public String getInsertNotificationChannel(String queueName) {
        return "pgmq.q_" + queueName + ".INSERT";
    }

    /**
     * Detects notification support without invoking the optional PGMQ function.
     */
    public NotificationCapability getNotificationCapability() {
        List<NotificationCapability> capabilities = jdbcTemplate.query(
                """
                SELECT pg_extension_entry.extversion,
                       EXISTS (
                           SELECT 1
                           FROM pg_proc procedure_entry
                           JOIN pg_namespace namespace_entry
                             ON namespace_entry.oid = procedure_entry.pronamespace
                           WHERE namespace_entry.nspname = 'pgmq'
                             AND procedure_entry.proname = 'enable_notify_insert'
                       ) AS notify_supported
                FROM pg_extension pg_extension_entry
                WHERE pg_extension_entry.extname = 'pgmq'
                """,
                (resultSet, rowNum) -> {
                    String version = resultSet.getString("extversion");
                    boolean supported = resultSet.getBoolean("notify_supported");
                    String detail = supported
                            ? "PGMQ " + version + " exposes enable_notify_insert"
                            : "PGMQ " + version + " does not expose enable_notify_insert";
                    return new NotificationCapability(supported, version, detail);
                });
        if (capabilities.isEmpty()) {
            return new NotificationCapability(false, "unavailable", "PGMQ extension is not installed");
        }
        return capabilities.get(0);
    }

    /**
     * Returns the time until the earliest currently invisible queue message
     * becomes visible. This is used only to schedule listener wake-ups.
     */
    public Optional<Duration> getNextVisibleDelay(String queueName) {
        String queueTable = quoteIdentifier("q_" + queueName);
        String sql = """
                SELECT CEIL(EXTRACT(EPOCH FROM (MIN(vt) - clock_timestamp())) * 1000)::bigint
                FROM pgmq.%s
                WHERE vt > clock_timestamp()
                """.formatted(queueTable);
        Long delayMillis = jdbcTemplate.queryForObject(sql, Long.class);
        return delayMillis == null
                ? Optional.empty()
                : Optional.of(Duration.ofMillis(Math.max(1L, delayMillis)));
    }

    /**
     * Attempts to claim the short-lived cross-instance lease used to suppress
     * duplicate immediate reads after PostgreSQL broadcasts a notification.
     */
    public boolean tryClaimNotificationWakeup(
            String queueName,
            String ownerId,
            Duration leaseDuration) {
        List<String> claimedOwners = jdbcTemplate.query(
                """
                INSERT INTO pgmq_listener_wakeup_lease (queue_name, owner_id, lease_until)
                VALUES (?, ?, clock_timestamp() + (? * INTERVAL '1 millisecond'))
                ON CONFLICT (queue_name) DO UPDATE
                    SET owner_id = EXCLUDED.owner_id,
                        lease_until = EXCLUDED.lease_until
                    WHERE pgmq_listener_wakeup_lease.lease_until <= clock_timestamp()
                       OR pgmq_listener_wakeup_lease.owner_id = EXCLUDED.owner_id
                RETURNING owner_id
                """,
                (resultSet, rowNumber) -> resultSet.getString("owner_id"),
                queueName,
                ownerId,
                Math.max(1L, leaseDuration.toMillis()));
        return !claimedOwners.isEmpty() && ownerId.equals(claimedOwners.get(0));
    }

    private static String quoteIdentifier(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private void executeQueueCommand(String sql, String queueName) {
        jdbcTemplate.execute(
            sql,
            (PreparedStatementCallback<Void>) preparedStatement -> {
                preparedStatement.setString(1, queueName);
                preparedStatement.execute();
                return null;
            }
        );
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
            new PgmqMessageRowMapper<>(payloadCodec, type),
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
            new PgmqMessageRowMapper<>(payloadCodec, type),
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

    @NullMarked
    private record PgmqMessageRowMapper<T>(PgmqPayloadCodec payloadCodec,
                                           Class<T> type) implements RowMapper<PgmqMessage<T>> {
            @Override
            public PgmqMessage<T> mapRow(ResultSet rs, int rowNum) throws SQLException {
                try {
                    String messageJson = rs.getString("message");
                    T payload = payloadCodec.read(messageJson, type);

                    return PgmqMessage.<T>builder()
                            .msgId(rs.getLong("msg_id"))
                            .readCount(rs.getInt("read_ct"))
                            .enqueuedAt(rs.getObject("enqueued_at", OffsetDateTime.class))
                            .vt(rs.getObject("vt", OffsetDateTime.class))
                            .payload(payload)
                            .build();
                } catch (PgmqPayloadException e) {
                    throw new SQLException("Failed to deserialize PGMQ message payload", e);
                }
            }
        }
}
