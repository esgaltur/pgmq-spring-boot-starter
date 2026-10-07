package io.github.esgaltur.pgmq.core;

/**
 * Turns message payloads into PGMQ's JSONB and back.
 *
 * <p>The starter does not choose a JSON library for the application: auto-configuration adapts the
 * application's own Jackson 3 or Jackson 2 mapper, so payloads follow the same modules and settings
 * (Kotlin, Java time, naming) as the rest of the application. See {@code spring.pgmq.json}.</p>
 */
public interface PgmqPayloadCodec {

    /**
     * Serializes a payload to a JSON document.
     *
     * @throws PgmqPayloadException when the payload cannot be written
     */
    String write(Object payload);

    /**
     * Reads a JSON document into the given payload type.
     *
     * @throws PgmqPayloadException when the document does not match the type
     */
    <T> T read(String json, Class<T> type);
}
