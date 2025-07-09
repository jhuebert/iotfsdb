package org.huebert.iotfsdb.api.grpc;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.mapstruct.EnumMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ValueMapping;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.regex.Pattern;

@Mapper(
//    unmappedTargetPolicy = ReportingPolicy.ERROR
)
public interface CommonMapper {

    @Mapping(target = "id", source = "series")
    @Mapping(target = "valuesList", source = "values")
    CommonProto.SeriesData toGrpc(InsertRequest request);

    @Mapping(target = "series", source = "id")
    @Mapping(target = "values", source = "valuesList")
    InsertRequest fromGrpc(CommonProto.SeriesData request);

    default Long toMilliseconds(Duration value) {
        long totalNanos = value.getSeconds() * 1_000_000_000L + value.getNanos();
        return totalNanos / 1_000_000L;
    }

    default Duration fromMilliseconds(Long value) {
        return Duration.newBuilder()
            .setSeconds(value / 1000)
            .setNanos((int) (value % 1000) * 1_000_000)
            .build();
    }

    default double doubleValue(Number value) {
        return value.doubleValue();
    }

    default Timestamp toTimestamp(ZonedDateTime value) {
        Instant instant = value.toInstant();
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }

    default StringValue toStringValue(Pattern pattern) {
        return StringValue.of(pattern.pattern());
    }

    default String toString(Pattern pattern) {
        return pattern.pattern();
    }

    default Pattern toPattern(StringValue pattern) {
        return Pattern.compile(pattern.getValue());
    }

    default Pattern toPattern(String pattern) {
        return Pattern.compile(pattern);
    }

    default DoubleValue toDoubleValue(Number value) {
        return value != null ? DoubleValue.of(value.doubleValue()) : null;
    }

    default Int32Value toInt32Value(Integer value) {
        return value != null ? Int32Value.of(value) : null;
    }

    default Int64Value toInt64Value(Long value) {
        return value != null ? Int64Value.of(value) : null;
    }

    default Integer toInteger(Int32Value value) {
        return value != null ? value.getValue() : null;
    }

    default Double toDouble(DoubleValue value) {
        return value.getValue();
    }

    @EnumMapping(nameTransformationStrategy = "prefix", configuration = "REDUCER_")
    CommonProto.Reducer toProto(Reducer value);

    @EnumMapping(nameTransformationStrategy = "stripPrefix", configuration = "REDUCER_")
    @ValueMapping(source = MappingConstants.ANY_REMAINING, target = MappingConstants.NULL)
    Reducer fromProto(CommonProto.Reducer value);

    @EnumMapping(nameTransformationStrategy = "prefix", configuration = "NUMBER_TYPE_")
    CommonProto.NumberType toProto(NumberType value);

    @EnumMapping(nameTransformationStrategy = "stripPrefix", configuration = "NUMBER_TYPE_")
    @ValueMapping(source = MappingConstants.ANY_REMAINING, target = MappingConstants.NULL)
    NumberType fromProto(CommonProto.NumberType value);

    @EnumMapping(nameTransformationStrategy = "prefix", configuration = "PARTITION_PERIOD_")
    CommonProto.PartitionPeriod toProto(PartitionPeriod value);

    @EnumMapping(nameTransformationStrategy = "stripPrefix", configuration = "PARTITION_PERIOD_")
    @ValueMapping(source = MappingConstants.ANY_REMAINING, target = MappingConstants.NULL)
    PartitionPeriod fromProto(CommonProto.PartitionPeriod value);

    @Mapping(target = "timestamp", source = "time")
    CommonProto.SeriesValue toGrpc(SeriesData data);

    SeriesFile fromGrpc(CommonProto.Series series);

    @Mapping(target = "interval", source = "interval")
    SeriesDefinition fromGrpc(CommonProto.SeriesDefinition series);

    List<CommonProto.Series> toGrpc(List<SeriesFile> series);

}
