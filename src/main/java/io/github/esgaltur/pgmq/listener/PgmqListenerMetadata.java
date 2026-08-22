package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListener;

import java.lang.reflect.Method;

/**
 * Immutable, fully resolved description of a single {@link PgmqListener} method.
 *
 * <p>The {@link PgmqListenerRegistrar} creates this metadata during Spring bean discovery. Runtime
 * listener processing can therefore operate on validated method and queue information without
 * repeatedly inspecting annotations, resolving configuration values, or inferring payload shapes.
 *
 * @param bean the fully initialized Spring bean on which the listener method is invoked
 * @param method the validated method carrying {@link PgmqListener}; it accepts exactly one
 *     supported listener argument
 * @param annotation the listener annotation supplying runtime options such as visibility timeout,
 *     batch size, retry policy, archiving, and polling backoff
 * @param payloadType the type used to deserialize a queue message body; for wrapped and batch
 *     arguments this is the innermost payload type {@code T}, rather than {@code PgmqMessage<T>} or
 *     {@code List<T>}
 * @param messageWrapped whether the method receives complete PGMQ message envelopes, including
 *     message metadata, instead of deserialized payload values only
 * @param batch whether one invocation receives a list of messages; when {@code false}, the method
 *     is invoked separately for each received message
 * @param queue the queue name after resolving configuration placeholders in the annotation value
 * @param deadLetterQueue the resolved dead-letter queue name; an empty value disables dead-letter
 *     routing for this listener
 * @param concurrency the configured number of concurrent workers requested for this listener; the
 *     runtime enforces a minimum of one worker
 */
record PgmqListenerMetadata(
        Object bean,
        Method method,
        PgmqListener annotation,
        Class<?> payloadType,
        boolean messageWrapped,
        boolean batch,
        String queue,
        String deadLetterQueue,
        int concurrency) {
}
