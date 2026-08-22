package io.github.esgaltur.pgmq.benchmark;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.postgresql.PGConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in comparison for the two listener wake-up strategies. This is deliberately
 * excluded from normal test runs because timings depend on the Docker host.
 *
 * Run with: mvn -Dpgmq.benchmark=true -Dtest=PgmqListenerModeComparisonTest test
 */
@Testcontainers
@EnabledIfSystemProperty(named = "pgmq.benchmark", matches = "true")
class PgmqListenerModeComparisonTest {

    private static final int SAMPLES = 20;
    private static final int BATCH_SAMPLES = 5;
    private static final long POLL_INTERVAL_MILLIS = 250L;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("ghcr.io/pgmq/pg18-pgmq:v1.10.0")
                    .asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("pgmq_benchmark")
            .withUsername("postgres")
            .withPassword("postgres");

    @BeforeAll
    static void initializeQueues() throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE");
            statement.execute("SELECT pgmq.create('benchmark_polling')");
            statement.execute("SELECT pgmq.create('benchmark_notify')");
            statement.execute("SELECT pgmq.enable_notify_insert('benchmark_notify', 0)");
        }
    }

    @Test
    void comparePollingAndNotify() throws Exception {
        List<Long> pollingLatencies = measurePollingLatencies();
        List<Long> notificationLatencies = measureNotificationLatencies();
        configureNotificationThrottle(250);
        measureBatchSend("benchmark_polling", 1_000);
        measureBatchSend("benchmark_notify", 1_000);
        List<Long> pollingBatchSends = new ArrayList<>();
        List<Long> notificationBatchSends = new ArrayList<>();
        for (int sample = 0; sample < BATCH_SAMPLES; sample++) {
            if (sample % 2 == 0) {
                pollingBatchSends.add(measureBatchSend("benchmark_polling", 10_000).toMillis());
                notificationBatchSends.add(measureBatchSend("benchmark_notify", 10_000).toMillis());
            } else {
                notificationBatchSends.add(measureBatchSend("benchmark_notify", 10_000).toMillis());
                pollingBatchSends.add(measureBatchSend("benchmark_polling", 10_000).toMillis());
            }
        }

        String comparison = String.format(
                "%nPGMQ listener comparison (%d controlled low-traffic samples)%n" +
                "polling (%d ms interval): median=%d ms, p95=%d ms%n" +
                "LISTEN/NOTIFY:             median=%d ms, p95=%d ms%n" +
                "send_batch 10,000 messages without trigger: %d ms%n" +
                "send_batch 10,000 messages with trigger:    %d ms%n",
                SAMPLES,
                POLL_INTERVAL_MILLIS,
                percentile(pollingLatencies, 50),
                percentile(pollingLatencies, 95),
                percentile(notificationLatencies, 50),
                percentile(notificationLatencies, 95),
                percentile(pollingBatchSends, 50),
                percentile(notificationBatchSends, 50));

        System.out.println(comparison);
        assertTrue(percentile(notificationLatencies, 50) < POLL_INTERVAL_MILLIS,
                "Notification wake-up should beat the controlled polling interval");
    }

    private static List<Long> measurePollingLatencies() throws Exception {
        List<Long> latencies = new ArrayList<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection consumer = connection(); Connection producer = connection()) {
            for (int sample = 0; sample < SAMPLES; sample++) {
                CountDownLatch emptyReadCompleted = new CountDownLatch(1);
                Future<Long> result = executor.submit(() -> {
                    Long messageId = readOne(consumer, "benchmark_polling");
                    if (messageId == null) {
                        emptyReadCompleted.countDown();
                    }
                    while (messageId == null) {
                        Thread.sleep(POLL_INTERVAL_MILLIS);
                        messageId = readOne(consumer, "benchmark_polling");
                    }
                    return messageId;
                });

                assertTrue(emptyReadCompleted.await(2, TimeUnit.SECONDS));
                long startedAt = System.nanoTime();
                send(producer, "benchmark_polling", sample);
                Long messageId = result.get(2, TimeUnit.SECONDS);
                latencies.add(elapsedMillis(startedAt));
                archive(consumer, "benchmark_polling", messageId);
            }
        } finally {
            executor.shutdownNow();
        }
        return latencies;
    }

    private static List<Long> measureNotificationLatencies() throws Exception {
        List<Long> latencies = new ArrayList<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection listener = connection(); Connection producer = connection();
             Connection consumer = connection(); Statement statement = listener.createStatement()) {
            statement.execute("LISTEN \"pgmq.q_benchmark_notify.INSERT\"");
            PGConnection pgConnection = listener.unwrap(PGConnection.class);

            for (int sample = 0; sample < SAMPLES; sample++) {
                pgConnection.getNotifications();
                CountDownLatch waiting = new CountDownLatch(1);
                Future<Long> result = executor.submit(() -> {
                    waiting.countDown();
                    org.postgresql.PGNotification[] notifications = pgConnection.getNotifications(2_000);
                    assertTrue(notifications != null && notifications.length > 0,
                            "Notification was not received");
                    return readOne(consumer, "benchmark_notify");
                });

                assertTrue(waiting.await(2, TimeUnit.SECONDS));
                long startedAt = System.nanoTime();
                send(producer, "benchmark_notify", sample);
                Long messageId = result.get(2, TimeUnit.SECONDS);
                assertTrue(messageId != null, "Notification arrived but the message was not visible");
                latencies.add(elapsedMillis(startedAt));
                archive(consumer, "benchmark_notify", messageId);
            }
        } finally {
            executor.shutdownNow();
        }
        return latencies;
    }

    private static void configureNotificationThrottle(int throttleIntervalMillis) throws Exception {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE pgmq.notify_insert_throttle SET throttle_interval_ms = ? WHERE queue_name = ?")) {
            statement.setInt(1, throttleIntervalMillis);
            statement.setString(2, "benchmark_notify");
            statement.executeUpdate();
        }
    }

    private static Duration measureBatchSend(String queue, int messageCount) throws Exception {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FROM pgmq.send_batch(?, " +
                        "ARRAY(SELECT jsonb_build_object('sample', value) FROM generate_series(1, ?) value))")) {
            statement.setString(1, queue);
            statement.setInt(2, messageCount);
            long startedAt = System.nanoTime();
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
            }
            Duration duration = Duration.ofNanos(System.nanoTime() - startedAt);
            try (PreparedStatement purge = connection.prepareStatement("SELECT pgmq.purge_queue(?)")) {
                purge.setString(1, queue);
                purge.execute();
            }
            return duration;
        }
    }

    private static void send(Connection connection, String queue, int sample) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pgmq.send(?, jsonb_build_object('sample', ?))")) {
            statement.setString(1, queue);
            statement.setInt(2, sample);
            statement.execute();
        }
    }

    private static Long readOne(Connection connection, String queue) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT msg_id FROM pgmq.read(?, 30, 1)")) {
            statement.setString(1, queue);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : null;
            }
        }
    }

    private static void archive(Connection connection, String queue, long messageId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pgmq.archive(?, ?)")) {
            statement.setString(1, queue);
            statement.setLong(2, messageId);
            statement.execute();
        }
    }

    private static Connection connection() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private static long elapsedMillis(long startedAt) {
        return Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
    }

    private static long percentile(List<Long> values, int percentile) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int index = Math.max(0, (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1);
        return sorted.get(index);
    }
}
