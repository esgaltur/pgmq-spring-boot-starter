package io.github.esgaltur.pgmq.listener;

import io.micrometer.core.instrument.MeterRegistry;
import io.github.esgaltur.pgmq.annotation.PgmqListener;
import io.github.esgaltur.pgmq.config.PgmqProperties;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PgmqListenerProcessorTest {

    private PgmqTemplate pgmqTemplate;
    private PgmqIdempotencyRepository idempotencyRepository;
    private PgmqProperties pgmqProperties;
    private MeterRegistry meterRegistry;
    private PgmqListenerWakeupStrategy wakeupStrategy;
    private PgmqListenerRegistrar listenerRegistrar;
    private PgmqListenerProcessor processor;

    @BeforeEach
    void setUp() {
        pgmqTemplate = mock(PgmqTemplate.class);
        idempotencyRepository = mock(PgmqIdempotencyRepository.class);
        pgmqProperties = new PgmqProperties();
        pgmqProperties.setAutoCreateQueue(true);
        pgmqProperties.setListenerMode(PgmqProperties.ListenerMode.POLLING);
        pgmqProperties.setShutdownTimeout(Duration.ofSeconds(1));
        
        meterRegistry = mock(MeterRegistry.class);
        wakeupStrategy = new PgmqPollingListenerWakeupStrategy();
        listenerRegistrar = new PgmqListenerRegistrar();
        
        processor = new PgmqListenerProcessor(
                listenerRegistrar, pgmqTemplate, idempotencyRepository,
                pgmqProperties, wakeupStrategy, meterRegistry);
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
        wakeupStrategy = new PgmqNotifyListenerWakeupStrategy(pgmqTemplate, dataSource, pgmqProperties);
        processor = new PgmqListenerProcessor(
                listenerRegistrar, pgmqTemplate, idempotencyRepository,
                pgmqProperties, wakeupStrategy, meterRegistry);
        doThrow(new RuntimeException("enable_notify_insert is unavailable"))
                .when(pgmqTemplate).enableInsertNotifications("test_q", 250);

        listenerRegistrar.postProcessAfterInitialization(new TestBean(), "testBean");

        assertDoesNotThrow(() -> processor.start());
        assertTrue(processor.isRunning());
        verify(pgmqTemplate).enableInsertNotifications("test_q", 250);

        processor.stop();
    }

    static class TestBean {
        @PgmqListener(queue = "test_q")
        public void handle(String payload) {}
    }

    static class InvalidBean {
        @PgmqListener(queue = "test_q")
        public void handle(String payload, int extraParam) {}
    }
}
