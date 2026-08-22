package io.github.esgaltur.pgmq.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PgmqPropertiesTest {

    @Test
    void pollingIsTheBackwardCompatibleDefault() {
        assertEquals(PgmqProperties.ListenerMode.POLLING, new PgmqProperties().getListenerMode());
    }

    @Test
    void rejectsInvalidOperationalIntervals() {
        PgmqProperties properties = new PgmqProperties();
        properties.setNotificationRecoveryInterval(Duration.ZERO);

        assertThrows(IllegalArgumentException.class, properties::afterPropertiesSet);
    }

    @Test
    void rejectsThrottleIntervalsThatCannotBePassedToPgmq() {
        PgmqProperties properties = new PgmqProperties();
        properties.setNotificationThrottleInterval(Duration.ofMillis((long) Integer.MAX_VALUE + 1L));

        assertThrows(IllegalArgumentException.class, properties::afterPropertiesSet);
    }
}
