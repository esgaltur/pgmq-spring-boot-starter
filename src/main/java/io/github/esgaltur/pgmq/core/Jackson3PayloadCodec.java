package io.github.esgaltur.pgmq.core;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * {@link PgmqPayloadCodec} backed by a Jackson 3 ({@code tools.jackson}) {@link ObjectMapper}, the JSON
 * library Spring Boot 4 configures by default.
 */
public class Jackson3PayloadCodec implements PgmqPayloadCodec {

    private final ObjectMapper objectMapper;

    public Jackson3PayloadCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String write(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JacksonException e) {
            throw new PgmqPayloadException("Failed to serialize payload", e);
        }
    }

    @Override
    public <T> T read(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JacksonException e) {
            throw new PgmqPayloadException("Failed to deserialize PGMQ message payload", e);
        }
    }
}
