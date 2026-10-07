package io.github.esgaltur.pgmq;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
import io.github.esgaltur.pgmq.annotation.PgmqListenerMode;
import io.github.esgaltur.pgmq.core.PgmqMessage;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.github.esgaltur.pgmq.listener.PgmqListenerProcessor;
import io.github.esgaltur.pgmq.listener.PgmqListenerStatus;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = PgmqIntegrationTest.TestApplication.class)
@Testcontainers
public class PgmqIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
        DockerImageName.parse("ghcr.io/pgmq/pg18-pgmq:v1.10.0").asCompatibleSubstituteFor("postgres")
    )
        .withDatabaseName("pgmq_testdb")
        .withUsername("postgres")
        .withPassword("postgres");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.driver-class-name", postgres::getDriverClassName);
        registry.add("spring.pgmq.auto-create-queue", () -> "true");
        registry.add("spring.pgmq.listener-mode", () -> "notify");
        registry.add("spring.pgmq.notification-recovery-interval", () -> "30s");
        
        // Define SpEL properties for testing
        registry.add("app.queues.dynamic", () -> "spel_queue");
        registry.add("app.queues.concurrency", () -> "2");

        // Execute extension creation natively on the container BEFORE Spring Boot starts.
        try {
            postgres.execInContainer("psql", "-U", "postgres", "-d", "pgmq_testdb", "-c", "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize PGMQ extension", e);
        }
    }

    @Autowired
    private PgmqTemplate pgmqTemplate;

    @Autowired
    private PgmqIdempotencyRepository idempotencyRepository;

    @Autowired
    private PgmqListenerProcessor pgmqListenerProcessor;

    @Autowired
    private PgmqListenerStatus listenerStatus;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TestConsumer testConsumer;

    @Autowired
    private IdempotentTestConsumer idempotentConsumer;

    @Autowired
    private BatchTestConsumer batchConsumer;

    @Autowired
    private DlqTestConsumer dlqConsumer;

    @Autowired
    private TransactionalService transactionalService;

    @Autowired
    private BackoffTestConsumer backoffConsumer;

    @Autowired
    private ConcurrencyTestConsumer concurrencyConsumer;

    @Autowired
    private SpelTestConsumer spelConsumer;

    @Autowired
    private NotifyLatencyTestConsumer notifyLatencyConsumer;

    @Autowired
    private ThrottledNotificationTestConsumer throttledNotificationConsumer;

    @Autowired
    private PollingOverrideConsumer pollingOverrideConsumer;

    @Autowired
    private FailoverConsumer failoverConsumer;

    @Autowired
    private ScheduledVisibilityConsumer scheduledVisibilityConsumer;

    @Autowired
    private TransactionRollbackConsumer transactionRollbackConsumer;

    @Test
    void testManualProduceAndConsume() {
        String queueName = "manual_ops_queue";
        pgmqTemplate.createQueue(queueName);

        // 1. Send Message
        TestPayload payload = new TestPayload("manual_test_event", 1);
        long msgId = pgmqTemplate.send(queueName, payload);
        assertTrue(msgId > 0, "Message ID should be positive");

        // 2. Read Message
        List<PgmqMessage<TestPayload>> messages = pgmqTemplate.read(queueName, 30, 1, TestPayload.class);
        assertEquals(1, messages.size());

        // 3. Archive
        boolean archived = pgmqTemplate.archive(queueName, msgId);
        assertTrue(archived);
    }

    @Test
    void testPopMessage() {
        String queueName = "pop_queue";
        pgmqTemplate.createQueue(queueName);

        long msgId = pgmqTemplate.send(queueName, new TestPayload("pop_event", 99));

        Optional<PgmqMessage<TestPayload>> poppedOpt = pgmqTemplate.pop(queueName, TestPayload.class);
        assertTrue(poppedOpt.isPresent());
        assertEquals(msgId, poppedOpt.get().getMsgId());
    }

    @Test
    void testAnnotationDrivenListener() throws InterruptedException {
        String queueName = "listener_queue";
        
        pgmqTemplate.send(queueName, new TestPayload("background_event", 100));

        boolean processed = testConsumer.getLatch().await(5, TimeUnit.SECONDS);
        assertTrue(processed, "The @PgmqListener did not process the message");
        assertEquals("background_event", testConsumer.getReceivedPayload().getName());
    }

    @Test
    void testIdempotentListener() throws InterruptedException {
        String queueName = "idempotent_queue";
        
        // PAUSE polling momentarily to guarantee deterministic setup 
        // without race conditions where the thread reads msg1 before we mark it.
        pgmqListenerProcessor.stop();

        long msgId1 = pgmqTemplate.send(queueName, new TestPayload("skipped_event", 500));
        idempotencyRepository.markProcessed(queueName, msgId1);

        long msgId2 = pgmqTemplate.send(queueName, new TestPayload("processed_event", 600));

        // RESUME polling
        pgmqListenerProcessor.start();

        boolean processed = idempotentConsumer.getLatch().await(5, TimeUnit.SECONDS);
        assertTrue(processed, "The idempotent listener did not process the valid message");
        assertEquals("processed_event", idempotentConsumer.getReceivedPayload().getName());
    }

    @Test
    void testBatchListener() throws InterruptedException {
        String queueName = "batch_queue";
        
        pgmqTemplate.send(queueName, new TestPayload("batch1", 1));
        pgmqTemplate.send(queueName, new TestPayload("batch2", 2));
        pgmqTemplate.send(queueName, new TestPayload("batch3", 3));

        boolean processed = batchConsumer.getLatch().await(5, TimeUnit.SECONDS);
        assertTrue(processed, "The batch listener did not process the messages");
        assertEquals(3, batchConsumer.getReceivedPayloads().size());
    }

    @Test
    void testPoisonPillDlq() throws InterruptedException {
        String queueName = "dlq_source_queue";
        String dlqName = "my_dlq";
        
        pgmqListenerProcessor.stop();
        
        // Send poison pill
        long msgId = pgmqTemplate.send(queueName, new TestPayload("poison", -1));
        
        // Simulate it being read repeatedly and failing (incrementing read_ct)
        // We read and ignore it 2 times, pushing read_ct to 2
        pgmqTemplate.read(queueName, 0, 1, TestPayload.class);
        pgmqTemplate.read(queueName, 0, 1, TestPayload.class);

        pgmqListenerProcessor.start();

        // The listener allows maxRetries=2. The next read inside the processor will be read_ct=3, 
        // which exceeds maxRetries. It should be routed to DLQ.
        
        // Wait a bit for background threads to process the DLQ routing
        Thread.sleep(2000);
        
        // Verify original queue is empty
        List<PgmqMessage<TestPayload>> sourceQueue = pgmqTemplate.read(queueName, 30, 1, TestPayload.class);
        assertEquals(0, sourceQueue.size());

        // Verify DLQ has the message
        List<PgmqMessage<TestPayload>> dlqQueue = pgmqTemplate.read(dlqName, 30, 1, TestPayload.class);
        assertEquals(1, dlqQueue.size());
        assertEquals("poison", dlqQueue.get(0).getPayload().getName());
    }

    @Test
    void testTransactionalOutbox() {
        String queueName = "tx_queue";
        pgmqTemplate.createQueue(queueName);

        assertThrows(RuntimeException.class, () -> {
            transactionalService.processAndFail(queueName, new TestPayload("tx_fail", 10));
        });

        // Because the transaction rolled back, the message should NOT be in the queue
        List<PgmqMessage<TestPayload>> messages = pgmqTemplate.read(queueName, 30, 1, TestPayload.class);
        assertEquals(0, messages.size(), "Message should have rolled back with the transaction");
    }

    @Test
    void testExponentialBackoff() throws InterruptedException {
        String queueName = "backoff_queue";
        
        pgmqListenerProcessor.stop();
        
        long msgId = pgmqTemplate.send(queueName, new TestPayload("backoff_test", 1));
        
        // Read 1: Set read_ct = 1, method throws, new VT should be 5s * (2.0 ^ 1) = 10s
        pgmqListenerProcessor.start();
        
        // Wait for the background thread to poll and throw exception once
        Thread.sleep(1500); 
        
        // The message should currently be invisible due to the exponential backoff VT (10s)
        List<PgmqMessage<TestPayload>> invisibleQueue = pgmqTemplate.read(queueName, 0, 1, TestPayload.class);
        assertEquals(0, invisibleQueue.size(), "Message should be invisible due to dynamic VT");
    }

    @Test
    void testConcurrentConsumers() throws InterruptedException {
        String queueName = "concurrent_queue";
        
        // Send 10 messages
        for (int i = 0; i < 10; i++) {
            pgmqTemplate.send(queueName, new TestPayload("concurrent", i));
        }

        // Wait for all 10 to be processed by the 3 concurrent threads
        boolean processed = concurrencyConsumer.getLatch().await(10, TimeUnit.SECONDS);
        assertTrue(processed, "Not all concurrent messages were processed in time");
    }

    @Test
    void testDelayedMessage() throws InterruptedException {
        String queueName = "delayed_queue";
        pgmqTemplate.createQueue(queueName);
        
        // Send message with a 2-second delay
        pgmqTemplate.sendWithDelay(queueName, new TestPayload("delayed_event", 1), 2);
        
        // Immediately try to read it - should be empty
        List<PgmqMessage<TestPayload>> immediateRead = pgmqTemplate.read(queueName, 30, 1, TestPayload.class);
        assertEquals(0, immediateRead.size(), "Message should not be visible immediately");
        
        // Wait 2.5 seconds
        Thread.sleep(2500);
        
        // Read again - should now be visible
        List<PgmqMessage<TestPayload>> delayedRead = pgmqTemplate.read(queueName, 30, 1, TestPayload.class);
        assertEquals(1, delayedRead.size(), "Message should be visible after delay expires");
        assertEquals("delayed_event", delayedRead.get(0).getPayload().getName());
    }

    @Test
    void testSpelListener() throws InterruptedException {
        // The queue name is injected from properties: "spel_queue"
        pgmqTemplate.send("spel_queue", new TestPayload("spel_event", 99));

        boolean processed = spelConsumer.getLatch().await(5, TimeUnit.SECONDS);
        assertTrue(processed, "The SpEL annotated listener did not process the message");
        assertEquals("spel_event", spelConsumer.getReceivedPayload().getName());
    }

    @Test
    void testNotificationWakesListenerBeforePollingInterval() throws InterruptedException {
        long startedAt = System.nanoTime();
        pgmqTemplate.send("notify_latency_queue", new TestPayload("notify_event", 1));

        boolean processed = notifyLatencyConsumer.getLatch().await(3, TimeUnit.SECONDS);
        long elapsedMillis = java.time.Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertTrue(processed, "LISTEN/NOTIFY did not wake the listener");
        assertTrue(elapsedMillis < 3_000,
                "Listener should wake well before its 10-second polling interval");
    }

    @Test
    void testThrottledInsertIsFoundByConfirmationScan() throws InterruptedException {
        pgmqTemplate.send("notify_throttle_queue", new TestPayload("first", 1));
        assertTrue(throttledNotificationConsumer.getFirstMessage().await(2, TimeUnit.SECONDS));

        // This insert normally falls inside PGMQ's 250 ms notification throttle window.
        pgmqTemplate.send("notify_throttle_queue", new TestPayload("second", 2));

        assertTrue(throttledNotificationConsumer.getAllMessages().await(2, TimeUnit.SECONDS),
                "A throttled insert was not found by the post-notification confirmation scan");
    }

    @Test
    void testPerQueuePollingOverrideAndCapabilityDetection() throws InterruptedException {
        PgmqTemplate.NotificationCapability capability = pgmqTemplate.getNotificationCapability();
        assertTrue(capability.supported(), capability.detail());
        assertEquals(PgmqListenerMode.POLLING,
                listenerStatus.snapshot().queues().get("polling_override_queue").effectiveMode());

        pgmqTemplate.send("polling_override_queue", new TestPayload("polled", 1));
        assertTrue(pollingOverrideConsumer.getLatch().await(3, TimeUnit.SECONDS));
    }

    @Test
    void testNotificationWakeupLeaseCoordinatesApplicationInstances() throws InterruptedException {
        String queueName = "notification_lease_probe";

        assertTrue(pgmqTemplate.tryClaimNotificationWakeup(
                queueName, "owner-a", Duration.ofMillis(300)));
        assertFalse(pgmqTemplate.tryClaimNotificationWakeup(
                queueName, "owner-b", Duration.ofMillis(300)),
                "A second instance must not perform the same immediate queue read");
        assertTrue(awaitCondition(
                () -> pgmqTemplate.tryClaimNotificationWakeup(
                        queueName, "owner-b", Duration.ofMillis(300)),
                Duration.ofSeconds(2)),
                "Another instance must be able to claim an expired wake-up lease");
    }

    @Test
    void testDelayedListenerWakesNearVisibilityTimestamp() throws InterruptedException {
        long startedAt = System.nanoTime();
        pgmqTemplate.sendWithDelay(
                "scheduled_visibility_queue", new TestPayload("scheduled", 1), 2);

        assertTrue(scheduledVisibilityConsumer.getLatch().await(6, TimeUnit.SECONDS),
                "Delayed listener did not wake near the message visibility timestamp");
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
        assertTrue(elapsedMillis >= 1_500 && elapsedMillis < 6_000,
                "Delayed message should not wait for the 30-second recovery scan");
        assertTrue(listenerStatus.snapshot().scheduledWakeups() > 0);
    }

    @Test
    void testListenConnectionReconnectsAfterBackendTermination() throws InterruptedException {
        assertTrue(awaitCondition(
                () -> listenerStatus.snapshot().connectionState()
                        == PgmqListenerStatus.ConnectionState.CONNECTED,
                Duration.ofSeconds(3)));
        long reconnectsBefore = listenerStatus.snapshot().reconnects();
        // Only the listening connection may carry the listener's name; pooled connections that once
        // listened must not (they used to keep it, and this test then killed the wrong session).
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'pgmq-listener'",
                Integer.class));

        Boolean terminated = jdbcTemplate.queryForObject(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE application_name = 'pgmq-listener'
                  AND pid <> pg_backend_pid()
                """,
                Boolean.class);
        assertEquals(Boolean.TRUE, terminated);

        pgmqTemplate.send("failover_queue", new TestPayload("during_disconnect", 1));

        assertTrue(awaitCondition(
                () -> listenerStatus.snapshot().reconnects() > reconnectsBefore,
                Duration.ofSeconds(8)), "LISTEN connection was not re-established");
        assertTrue(failoverConsumer.getLatch().await(5, TimeUnit.SECONDS),
                "Message inserted during the LISTEN gap was not recovered");
    }

    @Test
    void testListenerDatabaseWorkRollsBackWhenHandlingFails() throws InterruptedException {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS listener_transaction_probe (
                    value VARCHAR(100) NOT NULL
                )
                """);
        jdbcTemplate.update("DELETE FROM listener_transaction_probe");
        pgmqTemplate.send("transaction_rollback_queue", new TestPayload("rollback", 1));

        assertTrue(transactionRollbackConsumer.getInvoked().await(3, TimeUnit.SECONDS));
        assertTrue(awaitCondition(
                () -> countTransactionProbeRows() == 0,
                Duration.ofSeconds(2)),
                "Database writes from a failed listener invocation must roll back");
    }

    private int countTransactionProbeRows() {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM listener_transaction_probe", Integer.class);
        return count == null ? 0 : count;
    }

    private static boolean awaitCondition(
            BooleanSupplier condition,
            Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(50L);
        }
        return condition.getAsBoolean();
    }

    @Test
    void testQueueDepth() {
        String queueName = "depth_queue";
        pgmqTemplate.createQueue(queueName);
        
        assertEquals(0, pgmqTemplate.getQueueDepth(queueName));
        
        pgmqTemplate.send(queueName, new TestPayload("1", 1));
        pgmqTemplate.send(queueName, new TestPayload("2", 2));
        
        assertEquals(2, pgmqTemplate.getQueueDepth(queueName));
    }

    // --- Test Data Structures and Components ---

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestPayload {
        private String name;
        private int value;
    }

    /**
     * A mock Spring Component simulating user code consuming messages.
     */
    @Component
    public static class TestConsumer {
        private final CountDownLatch latch = new CountDownLatch(1);
        private TestPayload receivedPayload;

        @PgmqListener(queue = "listener_queue", pollInterval = 500)
        public void handleMessage(PgmqMessage<TestPayload> message) {
            this.receivedPayload = message.getPayload();
            this.latch.countDown();
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        public TestPayload getReceivedPayload() {
            return receivedPayload;
        }
    }

    @Component
    public static class IdempotentTestConsumer {
        private final CountDownLatch latch = new CountDownLatch(1);
        private TestPayload receivedPayload;

        @PgmqListener(queue = "idempotent_queue", pollInterval = 500, idempotent = true)
        public void handleMessage(PgmqMessage<TestPayload> message) {
            this.receivedPayload = message.getPayload();
            this.latch.countDown();
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        public TestPayload getReceivedPayload() {
            return receivedPayload;
        }
    }

    @Component
    public static class TransactionalService {
        @Autowired
        private PgmqTemplate pgmqTemplate;

        @Transactional
        public void processAndFail(String queue, TestPayload payload) {
            pgmqTemplate.send(queue, payload);
            throw new RuntimeException("Simulated database failure");
        }
    }

    @Getter
    @Component
    public static class BatchTestConsumer {
        private final CountDownLatch latch = new CountDownLatch(1);
        private List<TestPayload> receivedPayloads;

        @PgmqListener(queue = "batch_queue", pollInterval = 500, qty = 10)
        public void handleBatch(List<TestPayload> messages) {
            if (messages.size() == 3) {
                this.receivedPayloads = messages;
                this.latch.countDown();
            }
        }

    }

    @Component
    public static class DlqTestConsumer {
        @PgmqListener(queue = "dlq_source_queue", pollInterval = 500, maxRetries = 2, deadLetterQueue = "my_dlq")
        public void handleMessage(PgmqMessage<TestPayload> message) {
            throw new RuntimeException("Failing constantly to trigger DLQ");
        }
    }

    @Component
    public static class BackoffTestConsumer {
        @PgmqListener(queue = "backoff_queue", pollInterval = 500, vt = 5, backoffMultiplier = 2.0)
        public void handleMessage(PgmqMessage<TestPayload> message) {
            throw new RuntimeException("Failing to trigger backoff");
        }
    }

    @Getter
        @Component
        public static class ConcurrencyTestConsumer {
            private final CountDownLatch latch = new CountDownLatch(10);
    
            @PgmqListener(queue = "concurrent_queue", pollInterval = 500, concurrency = "3")
            public void handleMessage(TestPayload message) throws InterruptedException {
                // Simulate work to ensure threads run concurrently
                Thread.sleep(200);
                latch.countDown();
            }
    
            public CountDownLatch getLatch() { return latch; }
        }
    
        @Component
        public static class SpelTestConsumer {
            private final CountDownLatch latch = new CountDownLatch(1);
            private TestPayload receivedPayload;
    
            @PgmqListener(queue = "${app.queues.dynamic}", concurrency = "${app.queues.concurrency}", pollInterval = 500)
            public void handleMessage(TestPayload message) {
                this.receivedPayload = message;
                this.latch.countDown();
            }
    
            public CountDownLatch getLatch() { return latch; }
            public TestPayload getReceivedPayload() { return receivedPayload; }
        }

        @Getter
        @Component
        public static class NotifyLatencyTestConsumer {
            private final CountDownLatch latch = new CountDownLatch(1);

            @PgmqListener(queue = "notify_latency_queue", pollInterval = 10_000)
            public void handleMessage(TestPayload message) {
                latch.countDown();
            }
        }

        @Getter
        @Component
        public static class ThrottledNotificationTestConsumer {
            private final CountDownLatch firstMessage = new CountDownLatch(1);
            private final CountDownLatch allMessages = new CountDownLatch(2);

            @PgmqListener(queue = "notify_throttle_queue", pollInterval = 10_000)
            public void handleMessage(TestPayload message) {
                firstMessage.countDown();
                allMessages.countDown();
            }
        }

        @Getter
        @Component
        public static class PollingOverrideConsumer {
            private final CountDownLatch latch = new CountDownLatch(1);

            @PgmqListener(
                    queue = "polling_override_queue",
                    mode = PgmqListenerMode.POLLING,
                    pollInterval = 100)
            public void handle(TestPayload message) {
                latch.countDown();
            }
        }

        @Getter
        @Component
        public static class FailoverConsumer {
            private final CountDownLatch latch = new CountDownLatch(1);

            @PgmqListener(queue = "failover_queue", mode = PgmqListenerMode.NOTIFY)
            public void handle(TestPayload message) {
                latch.countDown();
            }
        }

        @Getter
        @Component
        public static class ScheduledVisibilityConsumer {
            private final CountDownLatch latch = new CountDownLatch(1);

            @PgmqListener(
                    queue = "scheduled_visibility_queue",
                    mode = PgmqListenerMode.NOTIFY,
                    pollInterval = 10_000)
            public void handle(TestPayload message) {
                latch.countDown();
            }
        }

        @Getter
        @Component
        public static class TransactionRollbackConsumer {
            private final CountDownLatch invoked = new CountDownLatch(1);
            private final JdbcTemplate jdbcTemplate;

            public TransactionRollbackConsumer(JdbcTemplate jdbcTemplate) {
                this.jdbcTemplate = jdbcTemplate;
            }

            @PgmqListener(queue = "transaction_rollback_queue", vt = 5)
            public void handle(TestPayload message) {
                jdbcTemplate.update(
                        "INSERT INTO listener_transaction_probe (value) VALUES (?)", message.getName());
                invoked.countDown();
                throw new IllegalStateException("Trigger transaction rollback");
            }
        }
    
        /**
         * Dummy application class required by @SpringBootTest to bootstrap the context.
         */    @SpringBootApplication
    @ComponentScan("io.github.esgaltur.pgmq")
    static class TestApplication {
        @Bean
        public com.fasterxml.jackson.databind.ObjectMapper objectMapper() {
            return new com.fasterxml.jackson.databind.ObjectMapper();
        }
    }
}
