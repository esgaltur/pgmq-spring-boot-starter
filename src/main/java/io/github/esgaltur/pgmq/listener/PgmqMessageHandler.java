package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
import io.github.esgaltur.pgmq.core.PgmqIdempotencyRepository;
import io.github.esgaltur.pgmq.core.PgmqMessage;
import io.github.esgaltur.pgmq.core.PgmqTemplate;
import org.springframework.transaction.support.TransactionOperations;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Invokes listener methods and finalizes their PGMQ messages in one database
 * transaction. Database work performed by the listener joins this transaction
 * when it uses the same transaction manager.
 */
public final class PgmqMessageHandler {

    enum Outcome {
        PROCESSED,
        DUPLICATE
    }

    private final PgmqTemplate pgmqTemplate;
    private final PgmqIdempotencyRepository idempotencyRepository;
    private final TransactionOperations transactions;

    public PgmqMessageHandler(
            PgmqTemplate pgmqTemplate,
            PgmqIdempotencyRepository idempotencyRepository,
            TransactionOperations transactions) {
        this.pgmqTemplate = pgmqTemplate;
        this.idempotencyRepository = idempotencyRepository;
        this.transactions = transactions;
    }

    Outcome processSingle(PgmqListenerMetadata metadata, PgmqMessage<?> message) {
        if (!metadata.annotation().transactional()) {
            if (isDuplicate(metadata, message)) {
                archiveOrDelete(metadata.annotation(), metadata.queue(), message.getMsgId());
                return Outcome.DUPLICATE;
            }
            // No transaction around the method: it may take long without holding a connection.
            invoke(metadata, metadata.messageWrapped() ? message : message.getPayload());
            transactions.executeWithoutResult(status -> markProcessedAndFinalize(metadata, message));
            return Outcome.PROCESSED;
        }
        Outcome outcome = transactions.execute(status -> {
            if (isDuplicate(metadata, message)) {
                archiveOrDelete(metadata.annotation(), metadata.queue(), message.getMsgId());
                return Outcome.DUPLICATE;
            }

            Object argument = metadata.messageWrapped() ? message : message.getPayload();
            invoke(metadata, argument);
            markProcessedAndFinalize(metadata, message);
            return Outcome.PROCESSED;
        });
        if (outcome == null) {
            throw new IllegalStateException("Listener transaction completed without an outcome");
        }
        return outcome;
    }

    int processBatch(PgmqListenerMetadata metadata, List<PgmqMessage<?>> messages) {
        if (!metadata.annotation().transactional()) {
            List<PgmqMessage<?>> eligible = new ArrayList<>(messages.size());
            for (PgmqMessage<?> message : messages) {
                if (isDuplicate(metadata, message)) {
                    archiveOrDelete(metadata.annotation(), metadata.queue(), message.getMsgId());
                } else {
                    eligible.add(message);
                }
            }
            if (eligible.isEmpty()) {
                return 0;
            }
            invoke(metadata, metadata.messageWrapped()
                    ? eligible
                    : eligible.stream().map(PgmqMessage::getPayload).toList());
            transactions.executeWithoutResult(status -> eligible.forEach(message -> markProcessedAndFinalize(metadata, message)));
            return eligible.size();
        }
        Integer processed = transactions.execute(status -> {
            List<PgmqMessage<?>> eligible = new ArrayList<>(messages.size());
            for (PgmqMessage<?> message : messages) {
                if (isDuplicate(metadata, message)) {
                    archiveOrDelete(metadata.annotation(), metadata.queue(), message.getMsgId());
                } else {
                    eligible.add(message);
                }
            }
            if (eligible.isEmpty()) {
                return 0;
            }

            Object argument = metadata.messageWrapped()
                    ? eligible
                    : eligible.stream().map(PgmqMessage::getPayload).toList();
            invoke(metadata, argument);
            eligible.forEach(message -> markProcessedAndFinalize(metadata, message));
            return eligible.size();
        });
        if (processed == null) {
            throw new IllegalStateException("Batch listener transaction completed without a result");
        }
        return processed;
    }

    void routeToDeadLetter(PgmqListenerMetadata metadata, PgmqMessage<?> message) {
        transactions.executeWithoutResult(status -> {
            if (!metadata.deadLetterQueue().isEmpty()) {
                pgmqTemplate.send(metadata.deadLetterQueue(), message.getPayload());
            }
            archiveOrDelete(metadata.annotation(), metadata.queue(), message.getMsgId());
        });
    }

    private boolean isDuplicate(PgmqListenerMetadata metadata, PgmqMessage<?> message) {
        return metadata.annotation().idempotent()
                && idempotencyRepository.isProcessed(metadata.queue(), message.getMsgId());
    }

    private void markProcessedAndFinalize(PgmqListenerMetadata metadata, PgmqMessage<?> message) {
        if (metadata.annotation().idempotent()) {
            idempotencyRepository.markProcessed(metadata.queue(), message.getMsgId());
        }
        archiveOrDelete(metadata.annotation(), metadata.queue(), message.getMsgId());
    }

    private void archiveOrDelete(PgmqListener annotation, String queue, long messageId) {
        boolean finalized = annotation.archive()
                ? pgmqTemplate.archive(queue, messageId)
                : pgmqTemplate.delete(queue, messageId);
        if (!finalized) {
            throw new IllegalStateException(
                    "PGMQ message " + messageId + " on queue " + queue + " could not be finalized");
        }
    }

    private static void invoke(PgmqListenerMetadata metadata, Object argument) {
        try {
            metadata.method().invoke(metadata.bean(), argument);
        } catch (InvocationTargetException exception) {
            throw new PgmqListenerInvocationException(exception.getTargetException());
        } catch (ReflectiveOperationException exception) {
            throw new PgmqListenerInvocationException(exception);
        }
    }

    static final class PgmqListenerInvocationException extends RuntimeException {
        PgmqListenerInvocationException(Throwable cause) {
            super(cause.getMessage(), cause);
        }
    }
}
