package org.huebert.iotfsdb.api.grpc.service;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoServicesMapper;
import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.proto.SeriesServiceGrpc;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.grpc.server.service.GrpcService;

import java.util.List;

@GrpcService
public class GrpcSeriesService extends SeriesServiceGrpc.SeriesServiceImplBase {

    private static final ProtoServicesMapper MAPPER = Mappers.getMapper(ProtoServicesMapper.class);

    private final SeriesService seriesService;

    public GrpcSeriesService(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @CaptureStats(
        id = "iotfsdb-grpc-series-find",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "grpc"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "find"),
        }
    )
    @Override
    public void findSeries(IotfsdbServices.FindSeriesRequest request, StreamObserver<IotfsdbServices.FindSeriesResponse> responseObserver) {
        FindSeriesRequest serviceRequest = MAPPER.fromGrpc(request);
        List<SeriesFile> serviceResponse = seriesService.findSeries(serviceRequest);
        responseObserver.onNext(MAPPER.toGrpc(serviceResponse));
        responseObserver.onCompleted();
    }

    @CaptureStats(
        id = "iotfsdb-grpc-series-create",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "grpc"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "create"),
        }
    )
    @Override
    public void createSeries(IotfsdbServices.CreateSeriesRequest request, StreamObserver<IotfsdbServices.CreateSeriesResponse> responseObserver) {
        SeriesFile seriesFile = MAPPER.fromGrpc(request);
        seriesService.createSeries(seriesFile);
        responseObserver.onNext(IotfsdbServices.CreateSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(
        id = "iotfsdb-grpc-series-delete",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "grpc"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "delete"),
        }
    )
    @Override
    public void deleteSeries(IotfsdbServices.DeleteSeriesRequest request, StreamObserver<IotfsdbServices.DeleteSeriesResponse> responseObserver) {
        seriesService.deleteSeries(request.getId());
        responseObserver.onNext(IotfsdbServices.DeleteSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(
        id = "iotfsdb-grpc-series-get",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "grpc"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "get"),
        }
    )
    @Override
    public void getSeries(IotfsdbServices.GetSeriesRequest request, StreamObserver<IotfsdbServices.GetSeriesResponse> responseObserver) {
        responseObserver.onNext(seriesService.findSeries(request.getId())
            .map(MAPPER::toGrpc)
            .orElseGet(IotfsdbServices.GetSeriesResponse::getDefaultInstance));
        responseObserver.onCompleted();
    }

}
