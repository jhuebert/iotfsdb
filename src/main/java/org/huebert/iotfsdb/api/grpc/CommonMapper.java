package org.huebert.iotfsdb.api.grpc;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.mapstruct.CollectionMappingStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.NullValueCheckStrategy;
import org.mapstruct.NullValuePropertyMappingStrategy;
import org.mapstruct.ReportingPolicy;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

@Mapper(
    collectionMappingStrategy = CollectionMappingStrategy.ADDER_PREFERRED,
    nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE,
    unmappedTargetPolicy = ReportingPolicy.ERROR
)
public interface CommonMapper {

    CommonProto.Status SUCCESS_STATUS = CommonProto.Status.newBuilder()
        .setSuccess(true)
        .build();

    default double doubleValue(Number value) {
        return value.doubleValue();
    }

    default Long toMilliseconds(Duration value) {
        return (value.getSeconds() * 1_000) + (value.getNanos() / 1_000_000);
    }

    default Duration fromMilliseconds(Long value) {
        return Duration.newBuilder()
            .setSeconds(value / 1_000)
            .setNanos((int) (value % 1_000) * 1_000_000)
            .build();
    }

    default Timestamp toTimestamp(ZonedDateTime value) {
        return Timestamp.newBuilder()
            .setSeconds(value.toEpochSecond())
            .setNanos(value.getNano())
            .build();
    }

    default ZonedDateTime fromTimestamp(Timestamp value) {
        return ZonedDateTime.ofInstant(
            Instant.ofEpochSecond(value.getSeconds(), value.getNanos()),
            ZoneId.of("UTC")
        );
    }

    default ZonedDateTime fromProto(CommonProto.Time value) {
        if (value.hasTimestamp()) {
            return fromTimestamp(value.getTimestamp());
        }
        Duration relativeTime = value.getRelativeTime();
        //TODO Add?
        return ZonedDateTime.now()
            .minusSeconds(relativeTime.getSeconds())
            .minusNanos(relativeTime.getNanos());
    }

    default String fromPattern(Pattern pattern) {
        return pattern.pattern();
    }

    default Pattern toPattern(String pattern) {
        return Pattern.compile(pattern);
    }

    SeriesFile fromProto(CommonProto.Series series);

    CommonProto.Series toProto(SeriesFile series);

    SeriesDefinition fromProto(CommonProto.SeriesDefinition series);

    @Mapping(target = "timestamp", source = "time")
    CommonProto.SeriesValue toProto(SeriesData data);

    @Mapping(target = "time", source = "timestamp")
    SeriesData fromProto(CommonProto.SeriesValue data);

    default CommonProto.Status getFailedStatus(Exception e) {
        //TODO Handle specific exceptions
        return CommonProto.Status.newBuilder()
            .setSuccess(false)
            .setMessage("Error processing request")
            .setCode(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR)
            .build();
    }

}
