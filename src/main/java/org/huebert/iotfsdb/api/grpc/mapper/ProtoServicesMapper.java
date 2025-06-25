package org.huebert.iotfsdb.api.grpc.mapper;

import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.mapstruct.CollectionMappingStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.mapstruct.NullValueCheckStrategy;

import java.util.List;

@Mapper(
//    unmappedTargetPolicy = ReportingPolicy.IGNORE,
    nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS,
    collectionMappingStrategy = CollectionMappingStrategy.SETTER_PREFERRED,
    uses = {
        ProtoCommonMapper.class,
        ProtoEnumsMapper.class,
        ProtoTypesMapper.class,
    }
)
public interface ProtoServicesMapper {

    @Mappings({
        @Mapping(target = "data.id", source = "series"),
        @Mapping(target = "data.valuesList", source = "values"),
    })
    IotfsdbServices.InsertDataRequest toGrpc(InsertRequest request);

    @Mappings({
        @Mapping(target = "criteria.id", source = "pattern"),
        @Mapping(target = "criteria.mutableMetadata", source = "metadata"),
        @Mapping(target = "criteria.metadata", ignore = true),
    })
    IotfsdbServices.FindSeriesRequest toGrpc(FindSeriesRequest request);

    @Mappings({
        @Mapping(target = "criteria", source = "series"),
        @Mapping(target = "intervalMs", source = "interval"),
//        @Mapping(target = "nullHandling.includeNull", source = "includeNull"),
//        @Mapping(target = "nullHandling.usePrevious", source = "usePrevious"),
//        @Mapping(target = "nullHandling.nullValue", source = "nullValue"),
        @Mapping(target = "highPrecision", source = "useBigDecimal"),
//        @Mapping(target = "timeRangePreset", source = "dateTimePreset"),
        @Mapping(target = "timeRange.from.timestamp", source = "from"),
        @Mapping(target = "timeRange.to.timestamp", source = "to"),
    })
    IotfsdbServices.FindDataRequest toGrpc(FindDataRequest request);

    record FindDataResponseWrapper(List<FindDataResponse> findDataResponses) {
    }

    record SeriesFileWrapper(List<SeriesFile> seriesFiles) {
    }

    IotfsdbServices.FindDataResponse toGrpc(FindDataResponseWrapper wrapper);

    IotfsdbServices.FindSeriesResponse toGrpc(SeriesFileWrapper wrapper);

    default IotfsdbServices.FindDataResponse toGrpcFindDataResponse(List<FindDataResponse> findDataResponses) {
        return toGrpc(new FindDataResponseWrapper(findDataResponses));
    }

    default IotfsdbServices.FindSeriesResponse toGrpc(List<SeriesFile> seriesFiles) {
        return toGrpc(new SeriesFileWrapper(seriesFiles));
    }

    @Mappings({
        @Mapping(target = "metadata", source = "criteria.metadata"),
        @Mapping(target = "pattern", source = "criteria.id"),
    })
    FindSeriesRequest fromGrpc(IotfsdbServices.FindSeriesRequest request);

    FindDataRequest fromGrpc(IotfsdbServices.FindDataRequest request);

    @Mappings({
        @Mapping(target = "definition", source = "series.definition"),
        @Mapping(target = "metadata", source = "series.metadata"),
    })
    SeriesFile fromGrpc(IotfsdbServices.CreateSeriesRequest request);

    @Mappings({
        @Mapping(target = "values", source = "data.valuesList"),
        @Mapping(target = "series", source = "data.id"),
    })
    InsertRequest fromGrpc(IotfsdbServices.InsertDataRequest request);

}
