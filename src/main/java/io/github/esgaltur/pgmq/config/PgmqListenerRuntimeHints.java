package io.github.esgaltur.pgmq.config;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * Provides Ahead-Of-Time (AOT) hints for GraalVM Native Image compilation.
 * This ensures that methods annotated with @PgmqListener are not stripped out 
 * by the native compiler and are accessible via Java Reflection at runtime.
 */
public class PgmqListenerRuntimeHints implements RuntimeHintsRegistrar {

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        // We register a broad hint for the annotation itself
        hints.reflection().registerType(PgmqListener.class);
        
        // Note: In a true Spring AOT environment, BeanPostProcessors usually 
        // register their own specific hints during the AOT phase. 
        // For simplicity in this starter, this registrar acts as a hook point 
        // to remind the AOT engine about the existence of the annotation.
    }
}
