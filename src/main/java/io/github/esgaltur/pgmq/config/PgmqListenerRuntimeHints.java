package io.github.esgaltur.pgmq.config;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * Registers the listener annotation as an AOT reflection hint and provides an
 * extension point for future generated hints. Applications must still verify
 * their listener methods and payload types with Spring's native-image tooling.
 */
@NullMarked
public class PgmqListenerRuntimeHints implements RuntimeHintsRegistrar {

    @Override
    public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
        hints.reflection().registerType(PgmqListener.class);
    }
}
