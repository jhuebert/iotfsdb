package org.huebert.iotfsdb.api.grpc.api;

import org.huebert.iotfsdb.api.grpc.proto.v1.api.SeriesServiceProto;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.mapstruct.CollectionMappingStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.NullValueCheckStrategy;

import java.util.List;

@Mapper(
//    unmappedTargetPolicy = ReportingPolicy.IGNORE,
    nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS,
    collectionMappingStrategy = CollectionMappingStrategy.SETTER_PREFERRED,
    uses = {
        CommonMapper.class,
    }
)
public interface SeriesServiceMapper {

    @Mapping(target = "criteria.id", source = "pattern")
    @Mapping(target = "criteria.mutableMetadata", source = "metadata")
    @Mapping(target = "criteria.metadata", ignore = true)
    SeriesServiceProto.FindSeriesRequest toGrpc(FindSeriesRequest request);

    SeriesServiceProto.FindSeriesResponse toGrpc(SeriesFileWrapper wrapper);

    default SeriesServiceProto.FindSeriesResponse toGrpc(List<SeriesFile> seriesFiles) {
        return toGrpc(new SeriesFileWrapper(seriesFiles));
    }

    @Mapping(target = "metadata", source = "criteria.metadata")
    @Mapping(target = "pattern", source = "criteria.id")
    FindSeriesRequest fromGrpc(SeriesServiceProto.FindSeriesRequest request);

    @Mapping(target = "definition", source = "series.definition")
    @Mapping(target = "metadata", source = "series.metadata")
    SeriesFile fromGrpc(SeriesServiceProto.CreateSeriesRequest request);

    record FindDataResponseWrapper(List<FindDataResponse> findDataResponses) {
    }

    record SeriesFileWrapper(List<SeriesFile> seriesFiles) {
    }

}
