package org.huebert.iotfsdb.api.grpc.api;

import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.DataServiceProto;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.mapstruct.CollectionMappingStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.NullValueCheckStrategy;
import org.mapstruct.NullValuePropertyMappingStrategy;
import org.mapstruct.ReportingPolicy;

@Mapper(
    collectionMappingStrategy = CollectionMappingStrategy.ADDER_PREFERRED,
    nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE,
    unmappedTargetPolicy = ReportingPolicy.ERROR,
    uses = {
        CommonMapper.class,
    }
)
public interface ServiceMapper {

    @Mapping(target = "criteria", source = "series")
    @Mapping(target = "size.size", source = "size")
    @Mapping(target = "size.interval", source = "interval")
    @Mapping(target = "nullHandler", expression = "java(getNullHandler(request))")
    @Mapping(target = "highPrecision", source = "useBigDecimal")
    @Mapping(target = "timeRange.start.timestamp", source = "from")
    @Mapping(target = "timeRange.end.timestamp", source = "to")
    @Mapping(target = "partition", ignore = true)
    DataServiceProto.FindDataRequest toProto(FindDataRequest request);

    default CommonProto.NullOption getNullOption(FindDataRequest request) {
        if (request.isUsePrevious()) {
            return CommonProto.NullOption.NULL_HANDLER_PREVIOUS;
        } else if (request.isIncludeNull()) {
            return CommonProto.NullOption.NULL_HANDLER_INCLUDE;
        } else {
            return CommonProto.NullOption.NULL_HANDLER_EXCLUDE;
        }
    }

    default CommonProto.NullHandler getNullHandler(FindDataRequest request) {
        CommonProto.NullHandler.Builder builder = CommonProto.NullHandler.newBuilder();
        builder.setNullOption(getNullOption(request));
        if (request.getNullValue() != null) {
            builder.setNullValue(request.getNullValue().doubleValue());
        }
        return builder.build();
    }

    @Mapping(target = "id", source = "pattern")
    CommonProto.SeriesCriteria toProto(FindSeriesRequest request);

    @Mapping(target = "pattern", source = "id")
    FindSeriesRequest fromProto(CommonProto.SeriesCriteria request);

    @Mapping(target = "id", source = "series.id")
    @Mapping(target = "values", source = "data")
    CommonProto.SeriesData toProto(FindDataResponse wrapper);

    @Mapping(target = "interval", source = "size.interval")
    @Mapping(target = "size", source = "size.size")
    @Mapping(target = "timezone", expression = "java(java.util.TimeZone.getTimeZone(\"UTC\"))")
    @Mapping(target = "from", source = "timeRange.start")
    @Mapping(target = "to", source = "timeRange.end")
    @Mapping(target = "dateTimePreset", ignore = true)
    @Mapping(target = "series", source = "criteria")
    @Mapping(target = "includeNull", expression = "java(request.getNullHandler().getNullOption() == CommonProto.NullOption.NULL_HANDLER_INCLUDE)")
    @Mapping(target = "usePrevious", expression = "java(request.getNullHandler().getNullOption() == CommonProto.NullOption.NULL_HANDLER_PREVIOUS)")
    @Mapping(target = "nullValue", source = "nullHandler.nullValue")
    @Mapping(target = "useBigDecimal", source = "highPrecision")
    FindDataRequest fromProto(DataServiceProto.FindDataRequest request);

    @Mapping(target = "series", source = "request.id")
    InsertRequest fromProto(CommonProto.SeriesData request, CommonProto.Reducer reducer);

}
