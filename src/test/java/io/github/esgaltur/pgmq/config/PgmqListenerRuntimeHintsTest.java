package io.github.esgaltur.pgmq.config;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
import org.junit.jupiter.api.Test;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PgmqListenerRuntimeHintsTest {

    @Test
    void shouldRegisterHintsForPgmqListener() {
        RuntimeHints hints = new RuntimeHints();
        PgmqListenerRuntimeHints registrar = new PgmqListenerRuntimeHints();
        
        registrar.registerHints(hints, getClass().getClassLoader());
        
        // Verify that the GraalVM AOT engine will preserve reflection metadata for our annotation
        assertTrue(RuntimeHintsPredicates.reflection().onType(PgmqListener.class).test(hints),
                "Runtime hints should contain reflection configuration for @PgmqListener");
    }
}
