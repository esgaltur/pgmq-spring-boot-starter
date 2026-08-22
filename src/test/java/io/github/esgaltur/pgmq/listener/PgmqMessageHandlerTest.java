package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.core.PgmqMessage;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PgmqMessageHandlerTest {

    private final PgmqTemplate template = mock(PgmqTemplate.class);
    private final PgmqIdempotencyRepository idempotencyRepository =
            mock(PgmqIdempotencyRepository.class);
    private final PgmqMessageHandler handler = new PgmqMessageHandler(
            template, idempotencyRepository, TransactionOperations.withoutTransaction());

    @Test
    void duplicateIsFinalizedWithoutInvokingBusinessCode() throws Exception {
        TestListener listener = new TestListener();
        PgmqListenerMetadata metadata = metadata(listener, "idempotent");
        PgmqMessage<String> message = message(7L, "duplicate");
        when(idempotencyRepository.isProcessed("orders", 7L)).thenReturn(true);
        when(template.archive("orders", 7L)).thenReturn(true);

        assertEquals(PgmqMessageHandler.Outcome.DUPLICATE, handler.processSingle(metadata, message));

        assertEquals(0, listener.invocations);
        verify(idempotencyRepository, never()).markProcessed("orders", 7L);
    }

    @Test
    void failedInvocationDoesNotFinalizeMessage() throws Exception {
        TestListener listener = new TestListener();
        PgmqListenerMetadata metadata = metadata(listener, "failing");

        assertThrows(PgmqMessageHandler.PgmqListenerInvocationException.class,
                () -> handler.processSingle(metadata, message(9L, "failure")));

        verify(template, never()).archive("orders", 9L);
        verify(template, never()).delete("orders", 9L);
    }

    @Test
    void deadLetterSendPrecedesSourceFinalization() throws Exception {
        TestListener listener = new TestListener();
        PgmqListenerMetadata metadata = metadata(listener, "deadLetter");
        PgmqMessage<String> message = message(11L, "poison");
        when(template.archive("orders", 11L)).thenReturn(true);

        handler.routeToDeadLetter(metadata, message);

        var ordered = inOrder(template);
        ordered.verify(template).send("orders_dlq", "poison");
        ordered.verify(template).archive("orders", 11L);
    }

    private static PgmqListenerMetadata metadata(TestListener listener, String methodName) throws Exception {
        Method method = TestListener.class.getDeclaredMethod(methodName, String.class);
        PgmqListener annotation = method.getAnnotation(PgmqListener.class);
        return new PgmqListenerMetadata(
                listener, method, annotation, String.class, false, false,
                "orders", annotation.deadLetterQueue(), 1);
    }

    private static PgmqMessage<String> message(long id, String payload) {
        return PgmqMessage.<String>builder().msgId(id).payload(payload).build();
    }

    static class TestListener {
        int invocations;

        @PgmqListener(queue = "orders", idempotent = true)
        void idempotent(String payload) {
            invocations++;
        }

        @PgmqListener(queue = "orders")
        void failing(String payload) {
            throw new IllegalStateException("failure");
        }

        @PgmqListener(queue = "orders", deadLetterQueue = "orders_dlq")
        void deadLetter(String payload) {
            invocations++;
        }
    }
}
