package org.huebert.iotfsdb.api.grpc.mapper;

import org.huebert.iotfsdb.api.proto.internal.IotfsdbInternal;
import org.huebert.iotfsdb.service.PartitionKey;
import org.mapstruct.CollectionMappingStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.NullValueCheckStrategy;

import java.util.List;
import java.util.Set;

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
public interface ProtoInternalMapper {

    PartitionKey fromGrpc(IotfsdbInternal.PartitionKey key);

    List<IotfsdbInternal.PartitionKey> toPartitionGrpc(Set<PartitionKey> partitions);

}
