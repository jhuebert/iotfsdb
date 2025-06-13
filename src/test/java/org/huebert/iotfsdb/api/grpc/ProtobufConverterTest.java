package org.huebert.iotfsdb.api.grpc;

import org.huebert.iotfsdb.api.grpc.mapper.ProtoServicesMapper;
import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;

public class ProtobufConverterTest {

    private static final ProtoServicesMapper MAPPER = Mappers.getMapper(ProtoServicesMapper.class);

    @Test
    public void test() throws Exception {
        FindDataRequest request = new FindDataRequest();
//        request.setDateTimePreset(DateTimePreset.LAST_2_DAYS);
        request.setFrom(ZonedDateTime.now().minusDays(1));
        request.setTo(ZonedDateTime.now());
//        request.setTimezone(TimeZone.getTimeZone("America/Chicago"));
//        request.getSeries().setPattern(Pattern.compile(".*(t6|t12).*"));
//        request.getSeries().getMetadata().put("device", Pattern.compile("(t6|t12)"));
//        request.getSeries().getMetadata().put("measurement", Pattern.compile("temperature"));
//        request.setInterval(60000L);
//        request.setSize(1000);
//        request.setNullValue(56.24);
        request.setIncludeNull(true);
//        request.setUseBigDecimal(true);
        request.setUsePrevious(true);
        request.setTimeReducer(Reducer.MEDIAN);
        request.setSeriesReducer(Reducer.MAXIMUM);
        byte[] protoBytes = MAPPER.toGrpc(request).toByteArray();
        System.out.println(Base64.getUrlEncoder().encodeToString(protoBytes));
        System.out.println(IotfsdbServices.FindDataRequest.parseFrom(protoBytes).toString());
    }

    @Test
    public void test2() throws Exception {
        InsertRequest request = InsertRequest.builder()
            .series("abc-123")
            .values(List.of(
                SeriesData.builder()
                    .time(ZonedDateTime.parse("2025-06-11T16:44:40-05:00"))
                    .value(12.34)
                    .build(),
                SeriesData.builder()
                    .time(ZonedDateTime.parse("2025-06-11T16:44:40-05:00"))
                    .value(56.78)
                    .build(),
                SeriesData.builder()
                    .time(ZonedDateTime.parse("2025-06-11T16:44:40-05:00"))
                    .value(90.12)
                    .build(),
                SeriesData.builder()
                    .time(ZonedDateTime.parse("2025-06-11T16:44:40-05:00"))
                    .value(34.56)
                    .build()
            ))
            .reducer(Reducer.LAST)
            .build();
        System.out.println(request);
        IotfsdbServices.InsertDataRequest grpc = MAPPER.toGrpc(request);
        System.out.println(grpc);
        byte[] protoBytes = grpc.toByteArray();
        System.out.println(Base64.getUrlEncoder().encodeToString(protoBytes));
        System.out.println(IotfsdbServices.InsertDataRequest.parseFrom(protoBytes).toString());
    }
}
