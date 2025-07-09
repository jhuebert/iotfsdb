package org.huebert.iotfsdb.api.grpc.api;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.SeriesServiceGrpc;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.SeriesServiceProto;
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

    private static final SeriesServiceMapper SERVICES_MAPPER = Mappers.getMapper(SeriesServiceMapper.class);

    private static final CommonMapper TYPES_MAPPER = Mappers.getMapper(CommonMapper.class);

    private final SeriesService seriesService;

    private final CloneService cloneService;

    public GrpcSeriesService(SeriesService seriesService, CloneService cloneService) {
        this.seriesService = seriesService;
        this.cloneService = cloneService;
    }

    @CaptureStats(group = "grpc", type = "series", operation = "find", javaClass = GrpcSeriesService.class, javaMethod = "findSeries")
    @Override
    public void findSeries(SeriesServiceProto.FindSeriesRequest request, StreamObserver<SeriesServiceProto.FindSeriesResponse> responseObserver) {
        FindSeriesRequest serviceRequest = SERVICES_MAPPER.fromGrpc(request);
        List<SeriesFile> serviceResponse = seriesService.findSeries(serviceRequest);
        responseObserver.onNext(SERVICES_MAPPER.toGrpc(serviceResponse));
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "create", javaClass = GrpcSeriesService.class, javaMethod = "createSeries")
    @Override
    public void createSeries(SeriesServiceProto.CreateSeriesRequest request, StreamObserver<SeriesServiceProto.CreateSeriesResponse> responseObserver) {
        SeriesFile seriesFile = SERVICES_MAPPER.fromGrpc(request);
        seriesService.createSeries(seriesFile);
        responseObserver.onNext(SeriesServiceProto.CreateSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "delete", javaClass = GrpcSeriesService.class, javaMethod = "deleteSeries")
    @Override
    public void deleteSeries(SeriesServiceProto.DeleteSeriesRequest request, StreamObserver<SeriesServiceProto.DeleteSeriesResponse> responseObserver) {
        seriesService.deleteSeries(request.getId());
        responseObserver.onNext(SeriesServiceProto.DeleteSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "clone", javaClass = GrpcSeriesService.class, javaMethod = "cloneSeries")
    @Override
    public void cloneSeries(SeriesServiceProto.CloneSeriesRequest request, StreamObserver<SeriesServiceProto.CloneSeriesResponse> responseObserver) {
        cloneService.cloneSeries(request.getSourceId(), request.getDestinationId());
        responseObserver.onNext(SeriesServiceProto.CloneSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "definition", operation = "update", javaClass = GrpcSeriesService.class, javaMethod = "updateDefinition")
    @Override
    public void updateDefinition(SeriesServiceProto.UpdateDefinitionRequest request, StreamObserver<SeriesServiceProto.UpdateDefinitionResponse> responseObserver) {
        cloneService.updateDefinition(request.getId(), TYPES_MAPPER.fromGrpc(request.getDefinition()));
        responseObserver.onNext(SeriesServiceProto.UpdateDefinitionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "metadata", operation = "update", javaClass = GrpcSeriesService.class, javaMethod = "updateMetadata")
    @Override
    public void updateMetadata(SeriesServiceProto.UpdateMetadataRequest request, StreamObserver<SeriesServiceProto.UpdateMetadataResponse> responseObserver) {
        seriesService.updateMetadata(request.getId(), request.getMetadataMap());
        responseObserver.onNext(SeriesServiceProto.UpdateMetadataResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
