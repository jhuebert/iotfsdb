package org.huebert.iotfsdb.api.grpc;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.ZonedDateTime;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CommonMapperTest {

    private static final CommonMapper MAPPER = Mappers.getMapper(CommonMapper.class);

    @Test
    void testToDoubleFromDoubleValue() {
        DoubleValue value = DoubleValue.of(3.14);
        assertEquals(3.14, MAPPER.toDouble(value));
    }

    @Test
    void testToDoubleFromNumber() {
        assertEquals(42.0, MAPPER.doubleValue(42));
        assertEquals(3.14, MAPPER.doubleValue(3.14));
    }

    @Test
    void testToIntegerFromInt32Value() {
        assertNull(MAPPER.toInteger(null));
        assertEquals(42, MAPPER.toInteger(Int32Value.of(42)));
    }

    @Test
    void testToDoubleValueFromNumber() {
        assertNull(MAPPER.toDoubleValue(null));
        assertEquals(42.0, MAPPER.toDoubleValue(42).getValue());
    }

    @Test
    void testToInt32ValueFromInteger() {
        assertNull(MAPPER.toInt32Value(null));
        assertEquals(42, MAPPER.toInt32Value(42).getValue());
    }

    @Test
    void testToInt64ValueFromLong() {
        assertNull(MAPPER.toInt64Value(null));
        assertEquals(123L, MAPPER.toInt64Value(123L).getValue());
    }

    @Test
    void testToPatternFromString() {
        String patternStr = "\\d+";
        Pattern pattern = MAPPER.toPattern(patternStr);
        assertNotNull(pattern);
        assertEquals(patternStr, pattern.pattern());
    }

    @Test
    void testToPatternFromStringValue() {
        StringValue patternValue = StringValue.of("\\d+");
        Pattern pattern = MAPPER.toPattern(patternValue);
        assertNotNull(pattern);
        assertEquals("\\d+", pattern.pattern());
    }

    @Test
    void testToStringFromPattern() {
        Pattern pattern = Pattern.compile("\\d+");
        assertEquals("\\d+", MAPPER.toString(pattern));
    }

    @Test
    void testToStringValueFromPattern() {
        Pattern pattern = Pattern.compile("\\d+");
        StringValue stringValue = MAPPER.toStringValue(pattern);
        assertNotNull(stringValue);
        assertEquals("\\d+", stringValue.getValue());
    }

    @Test
    void testToTimestamp() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        Timestamp timestamp = MAPPER.toTimestamp(zonedDateTime);
        assertEquals(zonedDateTime.toInstant().getEpochSecond(), timestamp.getSeconds());
        assertEquals(zonedDateTime.toInstant().getNano(), timestamp.getNanos());
    }


    @Test
    public void testReducerToProto() {
        assertEquals(CommonProto.Reducer.REDUCER_AVERAGE, MAPPER.toProto(Reducer.AVERAGE));
        assertEquals(CommonProto.Reducer.REDUCER_COUNT, MAPPER.toProto(Reducer.COUNT));
        assertEquals(CommonProto.Reducer.REDUCER_MAXIMUM, MAPPER.toProto(Reducer.MAXIMUM));
        assertEquals(CommonProto.Reducer.REDUCER_MINIMUM, MAPPER.toProto(Reducer.MINIMUM));
        assertEquals(CommonProto.Reducer.REDUCER_SUM, MAPPER.toProto(Reducer.SUM));
    }

    @Test
    public void testReducerFromProto() {
        assertEquals(Reducer.AVERAGE, MAPPER.fromProto(CommonProto.Reducer.REDUCER_AVERAGE));
        assertEquals(Reducer.COUNT, MAPPER.fromProto(CommonProto.Reducer.REDUCER_COUNT));
        assertEquals(Reducer.MAXIMUM, MAPPER.fromProto(CommonProto.Reducer.REDUCER_MAXIMUM));
        assertEquals(Reducer.MINIMUM, MAPPER.fromProto(CommonProto.Reducer.REDUCER_MINIMUM));
        assertEquals(Reducer.SUM, MAPPER.fromProto(CommonProto.Reducer.REDUCER_SUM));
        assertNull(MAPPER.fromProto(CommonProto.Reducer.REDUCER_UNSPECIFIED));
    }

    @Test
    public void testNumberTypeToProto() {
        assertEquals(CommonProto.NumberType.NUMBER_TYPE_CURVED1, MAPPER.toProto(NumberType.CURVED1));
        assertEquals(CommonProto.NumberType.NUMBER_TYPE_FLOAT4, MAPPER.toProto(NumberType.FLOAT4));
        assertEquals(CommonProto.NumberType.NUMBER_TYPE_INTEGER8, MAPPER.toProto(NumberType.INTEGER8));
        assertEquals(CommonProto.NumberType.NUMBER_TYPE_MAPPED2, MAPPER.toProto(NumberType.MAPPED2));
    }

    @Test
    public void testNumberTypeFromProto() {
        assertEquals(NumberType.CURVED1, MAPPER.fromProto(CommonProto.NumberType.NUMBER_TYPE_CURVED1));
        assertEquals(NumberType.FLOAT4, MAPPER.fromProto(CommonProto.NumberType.NUMBER_TYPE_FLOAT4));
        assertEquals(NumberType.INTEGER8, MAPPER.fromProto(CommonProto.NumberType.NUMBER_TYPE_INTEGER8));
        assertEquals(NumberType.MAPPED2, MAPPER.fromProto(CommonProto.NumberType.NUMBER_TYPE_MAPPED2));
        assertNull(MAPPER.fromProto(CommonProto.NumberType.NUMBER_TYPE_UNSPECIFIED));
    }

    @Test
    public void testPartitionPeriodToProto() {
        assertEquals(CommonProto.PartitionPeriod.PARTITION_PERIOD_DAY, MAPPER.toProto(PartitionPeriod.DAY));
        assertEquals(CommonProto.PartitionPeriod.PARTITION_PERIOD_MONTH, MAPPER.toProto(PartitionPeriod.MONTH));
        assertEquals(CommonProto.PartitionPeriod.PARTITION_PERIOD_YEAR, MAPPER.toProto(PartitionPeriod.YEAR));
    }

    @Test
    public void testPartitionPeriodFromProto() {
        assertEquals(PartitionPeriod.DAY, MAPPER.fromProto(CommonProto.PartitionPeriod.PARTITION_PERIOD_DAY));
        assertEquals(PartitionPeriod.MONTH, MAPPER.fromProto(CommonProto.PartitionPeriod.PARTITION_PERIOD_MONTH));
        assertEquals(PartitionPeriod.YEAR, MAPPER.fromProto(CommonProto.PartitionPeriod.PARTITION_PERIOD_YEAR));
        assertNull(MAPPER.fromProto(CommonProto.PartitionPeriod.PARTITION_PERIOD_UNSPECIFIED));
    }

}
