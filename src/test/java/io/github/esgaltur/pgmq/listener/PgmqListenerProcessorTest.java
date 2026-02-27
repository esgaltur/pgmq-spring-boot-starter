package io.github.esgaltur.pgmq.listener;

import io.micrometer.core.instrument.MeterRegistry;
import io.github.esgaltur.pgmq.annotation.PgmqListener;
import io.github.esgaltur.pgmq.config.PgmqProperties;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PgmqListenerProcessorTest {

    private PgmqTemplate pgmqTemplate;
    private PgmqIdempotencyRepository idempotencyRepository;
    private PgmqProperties pgmqProperties;
    private MeterRegistry meterRegistry;
    private PgmqListenerProcessor processor;

    @BeforeEach
    void setUp() {
        pgmqTemplate = mock(PgmqTemplate.class);
        idempotencyRepository = mock(PgmqIdempotencyRepository.class);
        pgmqProperties = new PgmqProperties();
        pgmqProperties.setAutoCreateQueue(true);
        pgmqProperties.setShutdownTimeout(Duration.ofSeconds(1));
        
        meterRegistry = mock(MeterRegistry.class);
        
        processor = new PgmqListenerProcessor(pgmqTemplate, idempotencyRepository, pgmqProperties, meterRegistry);
    }

    @Test
    void testAutoCreateQueueExceptionHandledGracefully() {
        // Setup a bean with the annotation
        TestBean bean = new TestBean();
        processor.postProcessAfterInitialization(bean, "testBean");

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
            processor.postProcessAfterInitialization(invalidBean, "invalidBean");
        });
        
        assertTrue(exception.getMessage().contains("exactly one parameter"));
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