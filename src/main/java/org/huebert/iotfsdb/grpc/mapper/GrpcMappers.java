package org.huebert.iotfsdb.grpc.mapper;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import org.huebert.iotfsdb.grpc.Iotfsdb;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.mapstruct.NullValueCheckStrategy;

import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

@Mapper(
    nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS
)
public interface GrpcMappers {

    @Mappings({
        @Mapping(target = "series", source = "pattern"),
        @Mapping(target = "mutableMetadata", source = "metadata"),
        @Mapping(target = "metadata", ignore = true)
    })
    Iotfsdb.FindSeriesRequest toGrpc(FindSeriesRequest request);

    Iotfsdb.FindDataRequest toGrpc(FindDataRequest request);

    Iotfsdb.Series toGrpc(SeriesFile seriesFile);

    record SeriesFileWrapper(List<SeriesFile> seriesFiles) {
    }

    Iotfsdb.FindSeriesResponse toGrpc(SeriesFileWrapper wrapper);

    default Iotfsdb.FindSeriesResponse toGrpc(List<SeriesFile> seriesFiles) {
        return toGrpc(new SeriesFileWrapper(seriesFiles));
    }

    @Mapping(target = "pattern", source = "series")
    FindSeriesRequest fromGrpc(Iotfsdb.FindSeriesRequest request);

    default String toString(Pattern pattern) {
        return pattern.pattern();
    }

    default Pattern toPattern(String pattern) {
        return Pattern.compile(pattern);
    }

    default String toString(TimeZone timeZone) {
        return timeZone.getID();
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

    default byte[] toBytes(Iotfsdb.FindDataRequest request) {
        return request.toByteArray();
    }

    default Iotfsdb.FindDataRequest toFindDataRequest(byte[] bytes) {
        try {
            return Iotfsdb.FindDataRequest.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

}
