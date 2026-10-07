package io.github.esgaltur.pgmq.core;

import org.jspecify.annotations.Nullable;

/** Chooses the {@link PgmqPayloadCodec} for an application (property {@code spring.pgmq.json}). */
public final class PgmqPayloadCodecs {

    /** Which Jackson the starter uses for payloads. */
    public enum Json {
        /**
         * The application's Jackson 3 mapper if it has one, otherwise its Jackson 2 mapper, otherwise a
         * default Jackson 3 mapper (Spring Boot 4's JSON library).
         */
        AUTO,
        /** The application's Jackson 2 mapper, or a default one with the modules found on the classpath. */
        JACKSON2,
        /** The application's Jackson 3 mapper, or a default one with the modules found on the classpath. */
        JACKSON3
    }

    private PgmqPayloadCodecs() {
    }

    public static PgmqPayloadCodec select(Json json,
                                          tools.jackson.databind.@Nullable ObjectMapper jackson3,
                                          com.fasterxml.jackson.databind.@Nullable ObjectMapper jackson2) {
        return switch (json) {
            case JACKSON3 -> new Jackson3PayloadCodec(jackson3 != null ? jackson3 : defaultJackson3());
            case JACKSON2 -> new Jackson2PayloadCodec(jackson2 != null ? jackson2 : defaultJackson2());
            case AUTO -> jackson3 != null ? new Jackson3PayloadCodec(jackson3)
                : jackson2 != null ? new Jackson2PayloadCodec(jackson2)
                : new Jackson3PayloadCodec(defaultJackson3());
        };
    }

    private static tools.jackson.databind.ObjectMapper defaultJackson3() {
        return tools.jackson.databind.json.JsonMapper.builder().findAndAddModules().build();
    }

    private static com.fasterxml.jackson.databind.ObjectMapper defaultJackson2() {
        return new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
    }
}
