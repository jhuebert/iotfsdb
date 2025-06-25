package org.huebert.iotfsdb.api.grpc.mapper;

import org.huebert.iotfsdb.api.proto.IotfsdbTypes;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
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
    }
)
public interface ProtoTypesMapper {

    @Mappings({
        @Mapping(target = "timestamp", source = "time"),
    })
    IotfsdbTypes.SeriesValue toGrpc(SeriesData data);

    SeriesFile fromGrpc(IotfsdbTypes.Series series);

    SeriesDefinition fromGrpc(IotfsdbTypes.SeriesDefinition series);

    List<IotfsdbTypes.Series> toGrpc(List<SeriesFile> series);

}
