package io.github.esgaltur.pgmq;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
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
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.github.esgaltur.pgmq.listener.PgmqListenerProcessor;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = PgmqIntegrationTest.TestApplication.class)
@Testcontainers
public class PgmqIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
        DockerImageName.parse("quay.io/tembo/pgmq-pg:latest").asCompatibleSubstituteFor("postgres")
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
