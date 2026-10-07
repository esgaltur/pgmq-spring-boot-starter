package io.github.esgaltur.pgmq.core;

import io.github.esgaltur.pgmq.core.PgmqPayloadCodecs.Json;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class PgmqPayloadCodecsTest {

    record Reminder(String device, int minuteOfDay, Instant due) {
    }

    private final tools.jackson.databind.ObjectMapper jackson3 = tools.jackson.databind.json.JsonMapper.builder().build();
    private final com.fasterxml.jackson.databind.ObjectMapper jackson2 =
        new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();

    @Test
    void autoPrefersTheApplicationsJackson3Mapper() {
        assertInstanceOf(Jackson3PayloadCodec.class, PgmqPayloadCodecs.select(Json.AUTO, jackson3, jackson2));
    }

    @Test
    void autoUsesTheApplicationsJackson2MapperWhenThatIsAllItHas() {
        assertInstanceOf(Jackson2PayloadCodec.class, PgmqPayloadCodecs.select(Json.AUTO, null, jackson2));
    }

    @Test
    void autoWithoutAnyMapperUsesJackson3SpringBoot4sDefault() {
        assertInstanceOf(Jackson3PayloadCodec.class, PgmqPayloadCodecs.select(Json.AUTO, null, null));
    }

    @Test
    void anExplicitChoiceWinsOverAuto() {
        assertInstanceOf(Jackson2PayloadCodec.class, PgmqPayloadCodecs.select(Json.JACKSON2, jackson3, jackson2));
        assertInstanceOf(Jackson3PayloadCodec.class, PgmqPayloadCodecs.select(Json.JACKSON3, null, jackson2));
        assertInstanceOf(Jackson2PayloadCodec.class, PgmqPayloadCodecs.select(Json.JACKSON2, null, null));
    }

    @Test
    void bothCodecsRoundTripARecordWithJavaTime() {
        Reminder reminder = new Reminder("device-1", 480, Instant.parse("2026-10-07T08:00:00Z"));
        for (PgmqPayloadCodec codec : new PgmqPayloadCodec[] {
            PgmqPayloadCodecs.select(Json.JACKSON3, null, null), PgmqPayloadCodecs.select(Json.JACKSON2, null, null)}) {
            assertEquals(reminder, codec.read(codec.write(reminder), Reminder.class), codec.getClass().getSimpleName());
        }
    }

    @Test
    void malformedJsonIsAPayloadExceptionWhicheverJackson() {
        for (PgmqPayloadCodec codec : new PgmqPayloadCodec[] {new Jackson3PayloadCodec(jackson3), new Jackson2PayloadCodec(jackson2)}) {
            assertThrows(PgmqPayloadException.class, () -> codec.read("{not json", Reminder.class), codec.getClass().getSimpleName());
        }
    }
}
