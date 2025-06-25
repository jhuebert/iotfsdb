package org.huebert.iotfsdb.api.grpc.service;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoServicesMapper;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoTypesMapper;
import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.proto.SeriesServiceGrpc;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.CloneService;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.grpc.server.service.GrpcService;

import java.util.List;

@GrpcService
public class GrpcSeriesService extends SeriesServiceGrpc.SeriesServiceImplBase {

    private static final ProtoServicesMapper SERVICES_MAPPER = Mappers.getMapper(ProtoServicesMapper.class);

    private static final ProtoTypesMapper TYPES_MAPPER = Mappers.getMapper(ProtoTypesMapper.class);

    private final SeriesService seriesService;

    private final CloneService cloneService;

    public GrpcSeriesService(SeriesService seriesService, CloneService cloneService) {
        this.seriesService = seriesService;
        this.cloneService = cloneService;
    }

    @CaptureStats(group = "grpc", type = "series", operation = "find", javaClass = GrpcSeriesService.class, javaMethod = "findSeries")
    @Override
    public void findSeries(IotfsdbServices.FindSeriesRequest request, StreamObserver<IotfsdbServices.FindSeriesResponse> responseObserver) {
        FindSeriesRequest serviceRequest = SERVICES_MAPPER.fromGrpc(request);
        List<SeriesFile> serviceResponse = seriesService.findSeries(serviceRequest);
        responseObserver.onNext(SERVICES_MAPPER.toGrpc(serviceResponse));
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "create", javaClass = GrpcSeriesService.class, javaMethod = "createSeries")
    @Override
    public void createSeries(IotfsdbServices.CreateSeriesRequest request, StreamObserver<IotfsdbServices.CreateSeriesResponse> responseObserver) {
        SeriesFile seriesFile = SERVICES_MAPPER.fromGrpc(request);
        seriesService.createSeries(seriesFile);
        responseObserver.onNext(IotfsdbServices.CreateSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "delete", javaClass = GrpcSeriesService.class, javaMethod = "deleteSeries")
    @Override
    public void deleteSeries(IotfsdbServices.DeleteSeriesRequest request, StreamObserver<IotfsdbServices.DeleteSeriesResponse> responseObserver) {
        seriesService.deleteSeries(request.getId());
        responseObserver.onNext(IotfsdbServices.DeleteSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "clone", javaClass = GrpcSeriesService.class, javaMethod = "cloneSeries")
    @Override
    public void cloneSeries(IotfsdbServices.CloneSeriesRequest request, StreamObserver<IotfsdbServices.CloneSeriesResponse> responseObserver) {
        cloneService.cloneSeries(request.getSourceId(), request.getDestinationId());
        responseObserver.onNext(IotfsdbServices.CloneSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "definition", operation = "update", javaClass = GrpcSeriesService.class, javaMethod = "updateDefinition")
    @Override
    public void updateDefinition(IotfsdbServices.UpdateDefinitionRequest request, StreamObserver<IotfsdbServices.UpdateDefinitionResponse> responseObserver) {
        cloneService.updateDefinition(request.getId(), TYPES_MAPPER.fromGrpc(request.getDefinition()));
        responseObserver.onNext(IotfsdbServices.UpdateDefinitionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "metadata", operation = "update", javaClass = GrpcSeriesService.class, javaMethod = "updateMetadata")
    @Override
    public void updateMetadata(IotfsdbServices.UpdateMetadataRequest request, StreamObserver<IotfsdbServices.UpdateMetadataResponse> responseObserver) {
        seriesService.updateMetadata(request.getId(), request.getMetadataMap());
        responseObserver.onNext(IotfsdbServices.UpdateMetadataResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
