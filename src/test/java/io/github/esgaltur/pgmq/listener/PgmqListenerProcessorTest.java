package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
import io.github.esgaltur.pgmq.annotation.PgmqListenerMode;
import io.github.esgaltur.pgmq.config.PgmqProperties;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.Duration;
import org.springframework.transaction.support.TransactionOperations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PgmqListenerProcessorTest {

    private PgmqTemplate pgmqTemplate;
    private PgmqIdempotencyRepository idempotencyRepository;
    private PgmqProperties pgmqProperties;
    private PgmqListenerMetrics listenerMetrics;
    private PgmqListenerWakeupStrategy wakeupStrategy;
    private PgmqListenerRegistrar listenerRegistrar;
    private PgmqListenerStatus listenerStatus;
    private PgmqMessageHandler messageHandler;
    private PgmqListenerProcessor processor;

    @BeforeEach
    void setUp() {
        pgmqTemplate = mock(PgmqTemplate.class);
        idempotencyRepository = mock(PgmqIdempotencyRepository.class);
        pgmqProperties = new PgmqProperties();
        pgmqProperties.setAutoCreateQueue(true);
        pgmqProperties.setListenerMode(PgmqProperties.ListenerMode.POLLING);
        pgmqProperties.setShutdownTimeout(Duration.ofSeconds(1));
        
        listenerMetrics = PgmqListenerMetrics.NO_OP;
        wakeupStrategy = new PgmqPollingListenerWakeupStrategy();
        listenerRegistrar = new PgmqListenerRegistrar();
        listenerStatus = new PgmqListenerStatus();
        messageHandler = new PgmqMessageHandler(
                pgmqTemplate, idempotencyRepository, TransactionOperations.withoutTransaction());
        
        processor = new PgmqListenerProcessor(
                listenerRegistrar, pgmqTemplate, messageHandler,
                pgmqProperties, wakeupStrategy, listenerStatus, listenerMetrics);
    }

    @Test
    void testAutoCreateQueueExceptionHandledGracefully() {
        // Setup a bean with the annotation
        TestBean bean = new TestBean();
        listenerRegistrar.postProcessAfterInitialization(bean, "testBean");

        // Simulate DB error on queue creation
        doThrow(new RuntimeException("DB Connection failed")).when(pgmqTemplate).createQueue("test_q");

        // Calling start should catch the exception and log a warning, rather than crashing
        assertDoesNotThrow(() -> processor.start());
        assertTrue(processor.isRunning());
        
        processor.stop();
    }

    @Test
    void testInvalidAnnotationSignature() {
        InvalidBean invalidBean = new InvalidBean();
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            listenerRegistrar.postProcessAfterInitialization(invalidBean, "invalidBean");
        });
        
        assertTrue(exception.getMessage().contains("exactly one parameter"));
    }

    @Test
    void fallsBackToPollingWhenNotificationsCannotBeEnabled() {
        pgmqProperties.setListenerMode(PgmqProperties.ListenerMode.NOTIFY);
        DataSource dataSource = mock(DataSource.class);
        when(pgmqTemplate.getNotificationCapability()).thenReturn(
                new PgmqTemplate.NotificationCapability(true, "1.10.0", "supported"));
        wakeupStrategy = new PgmqNotifyListenerWakeupStrategy(
                pgmqTemplate, dataSource, pgmqProperties, listenerStatus);
        processor = new PgmqListenerProcessor(
                listenerRegistrar, pgmqTemplate, messageHandler,
                pgmqProperties, wakeupStrategy, listenerStatus, listenerMetrics);
        doThrow(new RuntimeException("enable_notify_insert is unavailable"))
                .when(pgmqTemplate).enableInsertNotifications("test_q", 250);

        listenerRegistrar.postProcessAfterInitialization(new TestBean(), "testBean");

        assertDoesNotThrow(() -> processor.start());
        assertTrue(processor.isRunning());
        verify(pgmqTemplate).enableInsertNotifications("test_q", 250);
        assertEquals(PgmqListenerMode.POLLING,
                listenerStatus.snapshot().queues().get("test_q").effectiveMode());

        processor.stop();
    }

    @Test
    void rejectsConflictingModesForListenersSharingAQueue() {
        listenerRegistrar.postProcessAfterInitialization(new PollingBean(), "pollingBean");
        listenerRegistrar.postProcessAfterInitialization(new NotifyBean(), "notifyBean");

        IllegalStateException exception = assertThrows(IllegalStateException.class, processor::start);

        assertTrue(exception.getMessage().contains("same wake-up mode"));
    }

    @Test
    void rejectsInvalidListenerIntervalsDuringRegistration() {
        assertThrows(IllegalArgumentException.class,
                () -> listenerRegistrar.postProcessAfterInitialization(new InvalidIntervalBean(), "invalid"));
    }

    static class TestBean {
        @PgmqListener(queue = "test_q")
        public void handle(String payload) {}
    }

    static class InvalidBean {
        @PgmqListener(queue = "test_q")
        public void handle(String payload, int extraParam) {}
    }

    static class PollingBean {
        @PgmqListener(queue = "shared_q", mode = PgmqListenerMode.POLLING)
        public void handle(String payload) {}
    }

    static class NotifyBean {
        @PgmqListener(queue = "shared_q", mode = PgmqListenerMode.NOTIFY)
        public void handle(String payload) {}
    }

    static class InvalidIntervalBean {
        @PgmqListener(queue = "invalid_q", pollInterval = 0)
        public void handle(String payload) {}
    }
}
