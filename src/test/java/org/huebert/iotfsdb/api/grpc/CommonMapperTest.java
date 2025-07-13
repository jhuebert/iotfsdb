package org.huebert.iotfsdb.api.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import jakarta.validation.ValidationException;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

class CommonMapperTest {

    private CommonMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = Mappers.getMapper(CommonMapper.class);
    }

    @Test
    void testDoubleValue() {

        // Test with Integer
        assertEquals(42.0, mapper.doubleValue(42));

        // Test with Long
        assertEquals(42.0, mapper.doubleValue(42L));

        // Test with Float
        assertEquals(42.5, mapper.doubleValue(42.5f), 0.0001);

        // Test with Double
        assertEquals(42.5, mapper.doubleValue(42.5), 0.0001);
    }

    @Test
    void testToMilliseconds() {

        // Test regular case
        Duration duration = Duration.newBuilder()
            .setSeconds(3)
            .setNanos(500000000) // 500 ms
            .build();
        assertEquals(3500L, mapper.toMilliseconds(duration));

        // Test zero case
        Duration zeroDuration = Duration.newBuilder()
            .setSeconds(0)
            .setNanos(0)
            .build();
        assertEquals(0L, mapper.toMilliseconds(zeroDuration));

        // Test large value
        Duration largeDuration = Duration.newBuilder()
            .setSeconds(1000)
            .setNanos(999000000) // 999 ms
            .build();
        assertEquals(1000999L, mapper.toMilliseconds(largeDuration));
    }

    @Test
    void testFromMilliseconds() {

        // Test regular case
        Duration duration = mapper.fromMilliseconds(3500L);
        assertEquals(3, duration.getSeconds());
        assertEquals(500000000, duration.getNanos());

        // Test zero case
        Duration zeroDuration = mapper.fromMilliseconds(0L);
        assertEquals(0, zeroDuration.getSeconds());
        assertEquals(0, zeroDuration.getNanos());

        // Test large value
        Duration largeDuration = mapper.fromMilliseconds(1000999L);
        assertEquals(1000, largeDuration.getSeconds());
        assertEquals(999000000, largeDuration.getNanos());
    }

    @Test
    void testToTimestamp() {

        // Test specific date time
        ZonedDateTime dateTime = ZonedDateTime.of(2023, 1, 15, 10, 30, 45, 500000000, ZoneId.of("UTC"));
        Timestamp timestamp = mapper.toTimestamp(dateTime);

        // 2023-01-15 10:30:45.5 UTC
        long expectedSeconds = dateTime.toEpochSecond();
        int expectedNanos = dateTime.getNano();

        assertEquals(expectedSeconds, timestamp.getSeconds());
        assertEquals(expectedNanos, timestamp.getNanos());
    }

    @Test
    void testFromTimestamp() {

        // Create a timestamp for 2023-01-15 10:50:45.5 UTC
        Timestamp timestamp = Timestamp.newBuilder()
            .setSeconds(1673779845) // 2023-01-15 10:50:45 UTC
            .setNanos(500000000)    // 500 million nanoseconds = 0.5 seconds
            .build();

        ZonedDateTime dateTime = mapper.fromTimestamp(timestamp);

        assertEquals(2023, dateTime.getYear());
        assertEquals(1, dateTime.getMonthValue());
        assertEquals(15, dateTime.getDayOfMonth());
        assertEquals(10, dateTime.getHour());
        assertEquals(50, dateTime.getMinute());
        assertEquals(45, dateTime.getSecond());
        assertEquals(500000000, dateTime.getNano());
        assertEquals(ZoneId.of("UTC"), dateTime.getZone());
    }

    @Test
    void testFromProtoWithTimestamp() {

        // Create a time with timestamp
        Timestamp timestamp = Timestamp.newBuilder()
            .setSeconds(1673779845) // 2023-01-15 10:50:45 UTC
            .setNanos(500000000)    // 500 million nanoseconds = 0.5 seconds
            .build();

        CommonProto.Time time = CommonProto.Time.newBuilder()
            .setTimestamp(timestamp)
            .build();

        ZonedDateTime dateTime = mapper.fromProto(time);

        assertEquals(2023, dateTime.getYear());
        assertEquals(1, dateTime.getMonthValue());
        assertEquals(15, dateTime.getDayOfMonth());
        assertEquals(10, dateTime.getHour());
        assertEquals(50, dateTime.getMinute());
        assertEquals(45, dateTime.getSecond());
        assertEquals(500000000, dateTime.getNano());
    }

    @Test
    void testFromProtoWithRelativeTime() {

        // Create a time with relative time (3 minutes and 30 seconds ago)
        Duration relativeTime = Duration.newBuilder()
            .setSeconds(-210) // 3 minutes and 30 seconds
            .setNanos(0)
            .build();

        CommonProto.Time time = CommonProto.Time.newBuilder()
            .setRelativeTime(relativeTime)
            .build();

        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        ZonedDateTime dateTime = mapper.fromProto(time);

        // The difference should be close to 3 minutes and 30 seconds
        long secondsDiff = now.toEpochSecond() - dateTime.toEpochSecond();
        assertTrue(Math.abs(secondsDiff - 210) < 2, "Time difference should be approximately 210 seconds");
    }

    @Test
    void testFromPattern() {
        Pattern pattern = Pattern.compile("^test[0-9]+$");
        String patternString = mapper.fromPattern(pattern);
        assertEquals("^test[0-9]+$", patternString);
    }

    @Test
    void testToPattern() {
        String patternString = "^test[0-9]+$";
        Pattern pattern = mapper.toPattern(patternString);

        assertTrue(pattern.matcher("test123").matches());
        assertFalse(pattern.matcher("test").matches());
        assertFalse(pattern.matcher("testABC").matches());
    }

    @Test
    void testSeriesFileMappings() {

        // Create a SeriesFile
        SeriesDefinition definition = SeriesDefinition.builder()
            .id("test-series")
            .type(NumberType.FLOAT8)
            .partition(PartitionPeriod.DAY)
            .interval(60000L)
            .build();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(definition)
            .metadata(metadata)
            .build();

        // Convert to proto
        CommonProto.Series protoSeries = mapper.toProto(seriesFile);

        // Verify proto object
        assertEquals("test-series", protoSeries.getDefinition().getId());
        assertEquals(CommonProto.NumberType.NUMBER_TYPE_FLOAT8, protoSeries.getDefinition().getType());
        assertEquals(CommonProto.PartitionPeriod.PARTITION_PERIOD_DAY, protoSeries.getDefinition().getPartition());
        assertEquals(60L, protoSeries.getDefinition().getInterval().getSeconds());
        assertEquals(0, protoSeries.getDefinition().getInterval().getNanos());
        assertEquals("value1", protoSeries.getMetadataMap().get("key1"));
        assertEquals("value2", protoSeries.getMetadataMap().get("key2"));

        // Convert back to SeriesFile
        SeriesFile convertedSeriesFile = mapper.fromProto(protoSeries);

        // Verify converted object
        assertEquals("test-series", convertedSeriesFile.getDefinition().getId());
        assertEquals(NumberType.FLOAT8, convertedSeriesFile.getDefinition().getType());
        assertEquals(PartitionPeriod.DAY, convertedSeriesFile.getDefinition().getPartition());
        assertEquals(60000L, convertedSeriesFile.getDefinition().getInterval());
        assertEquals("value1", convertedSeriesFile.getMetadata().get("key1"));
        assertEquals("value2", convertedSeriesFile.getMetadata().get("key2"));
    }

    @Test
    void testSeriesDataMapping() {

        // Create a SeriesData
        ZonedDateTime time = ZonedDateTime.of(2023, 1, 15, 10, 30, 45, 0, ZoneId.of("UTC"));
        SeriesData seriesData = new SeriesData(time, 42.5);

        // Convert to proto
        CommonProto.SeriesValue protoValue = mapper.toProto(seriesData);

        // Verify proto object
        assertEquals(time.toEpochSecond(), protoValue.getTimestamp().getSeconds());
        assertEquals(time.getNano(), protoValue.getTimestamp().getNanos());
        assertEquals(42.5, protoValue.getValue(), 0.0001);

        // Convert back to SeriesData
        SeriesData convertedData = mapper.fromProto(protoValue);

        // Verify converted object
        assertEquals(time.toEpochSecond(), convertedData.getTime().toEpochSecond());
        assertEquals(time.getNano(), convertedData.getTime().getNano());
        assertEquals(42.5, convertedData.getValue().doubleValue(), 0.0001);
    }

    @Test
    void testGetFailedStatusWithValidationException() {
        ValidationException exception = new ValidationException("Invalid input");
        CommonProto.Status status = mapper.getFailedStatus(exception);

        assertFalse(status.getSuccess());
        assertEquals("Invalid input", status.getMessage());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_CLIENT_ERROR, status.getCode());
    }

    @Test
    void testGetFailedStatusWithGenericException() {
        RuntimeException exception = new RuntimeException("Server error");
        CommonProto.Status status = mapper.getFailedStatus(exception);

        assertFalse(status.getSuccess());
        assertEquals("Error processing request", status.getMessage());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, status.getCode());
    }

    @Test
    void testSuccessStatus() {
        CommonProto.Status status = CommonMapper.SUCCESS_STATUS;

        assertTrue(status.getSuccess());
        assertEquals("", status.getMessage());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_UNSPECIFIED, status.getCode());
    }
}
