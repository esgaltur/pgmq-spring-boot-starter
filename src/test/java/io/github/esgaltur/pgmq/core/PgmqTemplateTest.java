package io.github.esgaltur.pgmq.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PgmqTemplateTest {

    private JdbcTemplate jdbcTemplate;
    private ObjectMapper objectMapper;
    private PgmqTemplate pgmqTemplate;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        objectMapper = mock(ObjectMapper.class);
        pgmqTemplate = new PgmqTemplate(jdbcTemplate, objectMapper);
    }

    @Test
    void testSendSerializationException() throws JsonProcessingException {
        Object badPayload = new Object();
        when(objectMapper.writeValueAsString(badPayload)).thenThrow(new JsonProcessingException("Cannot serialize") {});

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            pgmqTemplate.send("test_q", badPayload);
        });

        assertTrue(exception.getMessage().contains("Failed to serialize payload"));
    }

    @Test
    void testPopEmptyQueue() {
        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq("empty_q")))
                .thenReturn(Collections.emptyList());

        Optional<PgmqMessage<String>> result = pgmqTemplate.pop("empty_q", String.class);
        assertFalse(result.isPresent());
    }

    @Test
    void testDeleteReturnsFalse() {
        when(jdbcTemplate.queryForObject(any(String.class), eq(Boolean.class), eq("test_q"), eq(1L)))
                .thenReturn(false);

        boolean result = pgmqTemplate.delete("test_q", 1L);
        assertFalse(result);
    }

    @Test
    void testArchiveReturnsFalse() {
        when(jdbcTemplate.queryForObject(any(String.class), eq(Boolean.class), eq("test_q"), eq(1L)))
                .thenReturn(false);

        boolean result = pgmqTemplate.archive("test_q", 1L);
        assertFalse(result);
    }
}