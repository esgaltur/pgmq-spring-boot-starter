package io.github.esgaltur.pgmq.core;

/**
 * Repository for tracking processed message IDs to ensure idempotent message consumption.
 */
public interface PgmqIdempotencyRepository {
    
    /**
     * Checks if a message has already been processed successfully.
     *
     * @param queueName the name of the queue
     * @param msgId the message ID
     * @return true if the message was already processed
     */
    boolean isProcessed(String queueName, long msgId);

    /**
     * Marks a message as processed.
     *
     * @param queueName the name of the queue
     * @param msgId the message ID
     */
    void markProcessed(String queueName, long msgId);
}
