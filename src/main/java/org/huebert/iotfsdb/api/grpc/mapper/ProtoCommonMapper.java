package org.huebert.iotfsdb.api.grpc.mapper;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

@Mapper(
    unmappedTargetPolicy = ReportingPolicy.ERROR
)
public interface ProtoCommonMapper {

    default double map(Number value) {
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

    default DoubleValue toDoubleValue(Number value) {
        return value != null ? DoubleValue.of(value.doubleValue()) : null;
    }

    default Int32Value toInt32Value(Integer value) {
        return value != null ? Int32Value.of(value) : null;
    }

    default Integer toInteger(Int32Value value) {
        return value != null ? value.getValue() : null;
    }

    default Double toDouble(DoubleValue value) {
        return value.getValue();
    }

}
