package org.huebert.iotfsdb.api.grpc.api;

import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.DataServiceProto;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.InsertRequest;
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
public interface DataServiceMapper {

    @Mapping(target = "data.id", source = "series")
    @Mapping(target = "data.valuesList", source = "values")
    DataServiceProto.InsertDataRequest toGrpc(InsertRequest request);

    @Mapping(target = "criteria", source = "series")
    @Mapping(target = "size.interval", source = "interval")
//        @Mapping(target = "nullHandling.includeNull", source = "includeNull"),
//        @Mapping(target = "nullHandling.usePrevious", source = "usePrevious"),
//        @Mapping(target = "nullHandling.nullValue", source = "nullValue"),
    @Mapping(target = "highPrecision", source = "useBigDecimal")
//        @Mapping(target = "timeRangePreset", source = "dateTimePreset"),
    @Mapping(target = "timeRange.start.timestamp", source = "from")
    @Mapping(target = "timeRange.end.timestamp", source = "to")
    DataServiceProto.FindDataRequest toGrpc(FindDataRequest request);

    DataServiceProto.FindDataResponse toGrpc(FindDataResponseWrapper wrapper);

    default DataServiceProto.FindDataResponse toGrpcFindDataResponse(List<FindDataResponse> findDataResponses) {
        return toGrpc(new FindDataResponseWrapper(findDataResponses));
    }

    @Mapping(target = "interval", source = "size.interval")
    @Mapping(target = "size", source = "size.size")
    FindDataRequest fromGrpc(DataServiceProto.FindDataRequest request);

    @Mapping(target = "values", source = "data.valuesList")
    @Mapping(target = "series", source = "data.id")
    InsertRequest fromGrpc(DataServiceProto.InsertDataRequest request);

    record FindDataResponseWrapper(List<FindDataResponse> findDataResponses) {
    }

    record SeriesFileWrapper(List<SeriesFile> seriesFiles) {
    }

}
