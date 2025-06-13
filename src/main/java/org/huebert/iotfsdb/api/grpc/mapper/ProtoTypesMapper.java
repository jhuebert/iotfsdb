package org.huebert.iotfsdb.api.grpc.mapper;

import org.huebert.iotfsdb.api.proto.IotfsdbTypes;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.mapstruct.CollectionMappingStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.mapstruct.NullValueCheckStrategy;

@Mapper(
//    unmappedTargetPolicy = ReportingPolicy.IGNORE,
    nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS,
    collectionMappingStrategy = CollectionMappingStrategy.SETTER_PREFERRED,
    uses = {
        ProtoCommonMapper.class,
    }
)
public interface ProtoTypesMapper {

    @Mappings({
        @Mapping(target = "timestamp", source = "time"),
    })
    IotfsdbTypes.SeriesValue toGrpc(SeriesData request);

}
