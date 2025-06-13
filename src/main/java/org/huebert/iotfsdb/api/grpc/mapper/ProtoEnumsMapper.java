package org.huebert.iotfsdb.api.grpc.mapper;

import org.huebert.iotfsdb.api.proto.IotfsdbEnums;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.mapstruct.EnumMapping;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.ValueMapping;

@Mapper(
    unmappedTargetPolicy = ReportingPolicy.ERROR
)
public interface ProtoEnumsMapper {

    @EnumMapping(nameTransformationStrategy = "prefix", configuration = "REDUCER_")
    IotfsdbEnums.Reducer toProto(Reducer value);

    @EnumMapping(nameTransformationStrategy = "stripPrefix", configuration = "REDUCER_")
    @ValueMapping(source = MappingConstants.ANY_REMAINING, target = MappingConstants.NULL)
    Reducer fromProto(IotfsdbEnums.Reducer value);

    @EnumMapping(nameTransformationStrategy = "prefix", configuration = "NUMBER_TYPE_")
    IotfsdbEnums.NumberType toProto(NumberType value);

    @EnumMapping(nameTransformationStrategy = "prefix", configuration = "PARTITION_PERIOD_")
    IotfsdbEnums.PartitionPeriod toProto(PartitionPeriod value);

}
