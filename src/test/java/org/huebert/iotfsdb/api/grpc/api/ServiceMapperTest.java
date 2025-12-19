package org.huebert.iotfsdb.api.grpc.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Duration;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.DataServiceProto;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

class ServiceMapperTest {

    private final ServiceMapper serviceMapper = Mappers.getMapper(ServiceMapper.class);

    private final CommonMapper commonMapper = Mappers.getMapper(CommonMapper.class);

    @Test
    void testGetNullOptionWithUsePrevious() {
        FindDataRequest request = new FindDataRequest();
        request.setUsePrevious(true);
        request.setIncludeNull(false);

        CommonProto.NullOption result = serviceMapper.getNullOption(request);

        assertEquals(CommonProto.NullOption.NULL_HANDLER_PREVIOUS, result);
    }

    @Test
    void testGetNullOptionWithIncludeNull() {
        FindDataRequest request = new FindDataRequest();
        request.setUsePrevious(false);
        request.setIncludeNull(true);

        CommonProto.NullOption result = serviceMapper.getNullOption(request);

        assertEquals(CommonProto.NullOption.NULL_HANDLER_INCLUDE, result);
    }

    @Test
    void testGetNullOptionDefault() {
        FindDataRequest request = new FindDataRequest();
        request.setUsePrevious(false);
        request.setIncludeNull(false);

        CommonProto.NullOption result = serviceMapper.getNullOption(request);

        assertEquals(CommonProto.NullOption.NULL_HANDLER_EXCLUDE, result);
    }

    @Test
    void testGetNullHandlerWithNullValue() {
        FindDataRequest request = new FindDataRequest();
        request.setUsePrevious(false);
        request.setIncludeNull(true);
        request.setNullValue(42.5);

        CommonProto.NullHandler result = serviceMapper.getNullHandler(request);

        assertEquals(CommonProto.NullOption.NULL_HANDLER_UNSPECIFIED, result.getNullOption());
        assertEquals(42.5, result.getNullValue(), 0.001);
    }

    @Test
    void testGetNullHandlerWithoutNullValue() {
        FindDataRequest request = new FindDataRequest();
        request.setUsePrevious(false);
        request.setIncludeNull(false);
        request.setNullValue(null);

        CommonProto.NullHandler result = serviceMapper.getNullHandler(request);

        assertEquals(CommonProto.NullOption.NULL_HANDLER_EXCLUDE, result.getNullOption());
        assertEquals(0.0, result.getNullValue(), 0.001);
    }

    @Test
    void testToProtoFindDataRequest() {
        ZonedDateTime from = ZonedDateTime.of(2023, 1, 15, 10, 0, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime to = ZonedDateTime.of(2023, 1, 15, 11, 0, 0, 0, ZoneId.of("UTC"));

        FindDataRequest request = new FindDataRequest();
        request.setFrom(from);
        request.setTo(to);
        request.setInterval(60000L);
        request.setSize(100);
        request.setUseBigDecimal(true);
        request.setIncludeNull(true);
        request.setNullValue(0.0);

        FindSeriesRequest seriesRequest = new FindSeriesRequest();
        seriesRequest.setPattern(Pattern.compile("test.*"));
        Map<String, Pattern> metadata = new HashMap<>();
        metadata.put("key1", Pattern.compile("value1"));
        seriesRequest.setMetadata(metadata);
        request.setSeries(seriesRequest);

        DataServiceProto.FindDataRequest protoRequest = serviceMapper.toProto(request);

        // Verify mapping
        assertEquals("test.*", protoRequest.getCriteria().getId());
        assertEquals(100, protoRequest.getSize().getSize());
        assertEquals(60L, protoRequest.getSize().getInterval().getSeconds());
        assertEquals(0, protoRequest.getSize().getInterval().getNanos());
        assertEquals(from.toEpochSecond(), protoRequest.getTimeRange().getStart().getTimestamp().getSeconds());
        assertEquals(to.toEpochSecond(), protoRequest.getTimeRange().getEnd().getTimestamp().getSeconds());
        assertTrue(protoRequest.getHighPrecision());
        assertEquals(CommonProto.NullOption.NULL_HANDLER_UNSPECIFIED, protoRequest.getNullHandler().getNullOption());
        assertEquals(0.0, protoRequest.getNullHandler().getNullValue(), 0.001);
    }

    @Test
    void testFromProtoFindDataRequest() {

        // Create proto request
        DataServiceProto.FindDataRequest.Builder builder = DataServiceProto.FindDataRequest.newBuilder();

        // Set time range
        ZonedDateTime from = ZonedDateTime.of(2023, 1, 15, 10, 0, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime to = ZonedDateTime.of(2023, 1, 15, 11, 0, 0, 0, ZoneId.of("UTC"));

        CommonProto.Time startTime = CommonProto.Time.newBuilder()
            .setTimestamp(commonMapper.toTimestamp(from))
            .build();

        CommonProto.Time endTime = CommonProto.Time.newBuilder()
            .setTimestamp(commonMapper.toTimestamp(to))
            .build();

        CommonProto.TimeRange timeRange = CommonProto.TimeRange.newBuilder()
            .setStart(startTime)
            .setEnd(endTime)
            .build();

        builder.setTimeRange(timeRange);

        // Set criteria
        CommonProto.SeriesCriteria criteria = CommonProto.SeriesCriteria.newBuilder()
            .setId("test.*")
            .build();
        builder.setCriteria(criteria);

        // Set size
        Duration interval = Duration.newBuilder()
            .setSeconds(60) // 60 seconds
            .build();
        CommonProto.Size size = CommonProto.Size.newBuilder()
            .setSize(100)
            .setInterval(interval)
            .build();
        builder.setSize(size);

        // Set null handler
        CommonProto.NullHandler nullHandler = CommonProto.NullHandler.newBuilder()
            .setNullOption(CommonProto.NullOption.NULL_HANDLER_INCLUDE)
            .build();
        builder.setNullHandler(nullHandler);

        // Set high precision
        builder.setHighPrecision(true);

        DataServiceProto.FindDataRequest protoRequest = builder.build();

        // Convert to domain object
        FindDataRequest request = serviceMapper.fromProto(protoRequest);

        // Verify mapping
        assertEquals("test.*", request.getSeries().getPattern().pattern());
        assertEquals(100, request.getSize().intValue());
        assertEquals(60000L, request.getInterval().longValue());
        assertEquals(TimeZone.getTimeZone("UTC"), request.getTimezone());
        assertTrue(request.isIncludeNull());
        assertFalse(request.isUsePrevious());
        assertNull(request.getNullValue());
        assertTrue(request.isUseBigDecimal());
    }

    @Test
    void testFromProtoFindDataRequestWithPreviousOption() {

        // Create proto request with previous null option
        DataServiceProto.FindDataRequest.Builder builder = DataServiceProto.FindDataRequest.newBuilder();

        // Set basic required fields
        CommonProto.SeriesCriteria criteria = CommonProto.SeriesCriteria.newBuilder()
            .setId("test.*")
            .build();
        builder.setCriteria(criteria);

        // Set null handler with previous option
        CommonProto.NullHandler nullHandler = CommonProto.NullHandler.newBuilder()
            .setNullOption(CommonProto.NullOption.NULL_HANDLER_PREVIOUS)
            .build();
        builder.setNullHandler(nullHandler);

        DataServiceProto.FindDataRequest protoRequest = builder.build();

        // Convert to domain object
        FindDataRequest request = serviceMapper.fromProto(protoRequest);

        // Verify mapping
        assertFalse(request.isIncludeNull());
        assertTrue(request.isUsePrevious());
    }

    @Test
    void testToProtoFindSeriesRequest() {
        FindSeriesRequest request = new FindSeriesRequest();
        request.setPattern(Pattern.compile("test.*"));

        CommonProto.SeriesCriteria criteria = serviceMapper.toProto(request);

        assertEquals("test.*", criteria.getId());
    }

    @Test
    void testFromProtoFindSeriesRequest() {
        CommonProto.SeriesCriteria criteria = CommonProto.SeriesCriteria.newBuilder()
            .setId("test.*")
            .build();

        FindSeriesRequest request = serviceMapper.fromProto(criteria);

        assertEquals("test.*", request.getPattern().pattern());
    }

    @Test
    void testToProtoFindDataResponse() {

        // Create a FindDataResponse
        SeriesDefinition definition = SeriesDefinition.builder()
            .id("test-series")
            .build();

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(definition)
            .metadata(new HashMap<>())
            .build();

        ZonedDateTime time = ZonedDateTime.of(2023, 1, 15, 10, 0, 0, 0, ZoneId.of("UTC"));
        List<SeriesData> dataList = new ArrayList<>();
        dataList.add(new SeriesData(time, 42.5));

        FindDataResponse response = new FindDataResponse(seriesFile, dataList);

        // Convert to proto
        CommonProto.SeriesData protoResponse = serviceMapper.toProto(response);

        // Verify mapping
        assertEquals("test-series", protoResponse.getId());
        assertEquals(1, protoResponse.getValuesCount());
        assertEquals(time.toEpochSecond(), protoResponse.getValues(0).getTimestamp().getSeconds());
        assertEquals(42.5, protoResponse.getValues(0).getValue(), 0.001);
    }

    @Test
    void testFromProtoInsertRequest() {

        // Create SeriesData proto
        CommonProto.SeriesValue value = CommonProto.SeriesValue.newBuilder()
            .setTimestamp(commonMapper.toTimestamp(ZonedDateTime.now()))
            .setValue(42.5)
            .build();

        List<CommonProto.SeriesValue> values = List.of(value);

        CommonProto.SeriesData seriesData = CommonProto.SeriesData.newBuilder()
            .setId("test-series")
            .addAllValues(values)
            .build();

        // Convert to domain object
        InsertRequest request = serviceMapper.fromProto(seriesData, CommonProto.Reducer.REDUCER_SUM);

        // Verify mapping
        assertEquals("test-series", request.getSeries());
        assertEquals(1, request.getValues().size());
        assertEquals(42.5, request.getValues().get(0).getValue().doubleValue(), 0.001);
        assertEquals(Reducer.SUM, request.getReducer());
    }
}
