package io.github.esgaltur.pgmq.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** {@link PgmqPayloadCodec} backed by a Jackson 2 ({@code com.fasterxml.jackson}) {@link ObjectMapper}. */
public class Jackson2PayloadCodec implements PgmqPayloadCodec {

    private final ObjectMapper objectMapper;

    public Jackson2PayloadCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String write(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new PgmqPayloadException("Failed to serialize payload", e);
        }
    }

    @Override
    public <T> T read(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new PgmqPayloadException("Failed to deserialize PGMQ message payload", e);
        }
    }
}
