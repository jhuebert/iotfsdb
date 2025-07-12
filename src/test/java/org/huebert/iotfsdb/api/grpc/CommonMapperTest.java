package org.huebert.iotfsdb.api.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.ZonedDateTime;
import java.util.regex.Pattern;

public class CommonMapperTest {

    private static final CommonMapper MAPPER = Mappers.getMapper(CommonMapper.class);

    @Test
    void testDoubleValue() {
        assertEquals(1.0, MAPPER.doubleValue(1));
        assertEquals(2.5, MAPPER.doubleValue(2.5));
        assertEquals(3.0, MAPPER.doubleValue(3L));
    }

    @Test
    void testToMilliseconds() {
        Duration duration = Duration.newBuilder().setSeconds(1).setNanos(500_000_000).build();
        assertEquals(1500, MAPPER.toMilliseconds(duration));

        duration = Duration.newBuilder().setSeconds(0).setNanos(0).build();
        assertEquals(0, MAPPER.toMilliseconds(duration));

        duration = Duration.newBuilder().setSeconds(-1).setNanos(500_000_000).build();
        assertEquals(-500, MAPPER.toMilliseconds(duration));
    }

    @Test
    void testFromMilliseconds() {
        Duration duration = MAPPER.fromMilliseconds(1500L);
        assertEquals(1, duration.getSeconds());
        assertEquals(500_000_000, duration.getNanos());

        duration = MAPPER.fromMilliseconds(0L);
        assertEquals(0, duration.getSeconds());
        assertEquals(0, duration.getNanos());
    }

    @Test
    void testToTimestamp() {
        ZonedDateTime now = ZonedDateTime.now();
        Timestamp timestamp = MAPPER.toTimestamp(now);
        assertEquals(now.toEpochSecond(), timestamp.getSeconds());
        assertEquals(now.getNano(), timestamp.getNanos());
    }

    @Test
    void testFromPattern() {
        Pattern pattern = Pattern.compile("^[a-zA-Z]+$");
        assertEquals("^[a-zA-Z]+$", MAPPER.fromPattern(pattern));
    }

    @Test
    void testToPattern() {
        Pattern pattern = MAPPER.toPattern("^[a-zA-Z]+$");
        assertNotNull(pattern);
    }

}
