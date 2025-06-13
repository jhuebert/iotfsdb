package org.huebert.iotfsdb.api.grpc.mapper;

import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
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
        @Mapping(target = "id", source = "series"),
        @Mapping(target = "valuesList", source = "values"),
    })
    IotfsdbServices.InsertDataRequest toGrpc(InsertRequest request);

    @Mappings({
        @Mapping(target = "id", source = "pattern"),
        @Mapping(target = "mutableMetadata", source = "metadata"),
        @Mapping(target = "metadata", ignore = true),
    })
    IotfsdbServices.FindSeriesRequest toGrpc(FindSeriesRequest request);

    @Mappings({
//        @Mapping(target = "nullHandling.includeNull", source = "includeNull"),
//        @Mapping(target = "nullHandling.usePrevious", source = "usePrevious"),
//        @Mapping(target = "nullHandling.nullValue", source = "nullValue"),
        @Mapping(target = "highPrecision", source = "useBigDecimal"),
//        @Mapping(target = "timeRangePreset", source = "dateTimePreset"),
        @Mapping(target = "fromTime.timestamp", source = "from"),
        @Mapping(target = "toTime.timestamp", source = "to"),
    })
    IotfsdbServices.FindDataRequest toGrpc(FindDataRequest request);

    record SeriesFileWrapper(List<SeriesFile> seriesFiles) {
    }

    IotfsdbServices.FindSeriesResponse toGrpc(SeriesFileWrapper wrapper);

    default IotfsdbServices.FindSeriesResponse toGrpc(List<SeriesFile> seriesFiles) {
        return toGrpc(new SeriesFileWrapper(seriesFiles));
    }

    @Mapping(target = "pattern", source = "id")
    FindSeriesRequest fromGrpc(IotfsdbServices.FindSeriesRequest request);

}
