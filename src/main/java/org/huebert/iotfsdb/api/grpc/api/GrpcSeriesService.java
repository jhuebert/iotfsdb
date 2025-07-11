package org.huebert.iotfsdb.api.grpc.api;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.SeriesServiceGrpc;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.SeriesServiceProto;
import org.huebert.iotfsdb.service.CloneService;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
public class GrpcSeriesService extends SeriesServiceGrpc.SeriesServiceImplBase {

    private static final ServiceMapper SERVICE_MAPPER = Mappers.getMapper(ServiceMapper.class);

    private static final CommonMapper MAPPER = Mappers.getMapper(CommonMapper.class);

    private final SeriesService seriesService;

    private final CloneService cloneService;

    public GrpcSeriesService(SeriesService seriesService, CloneService cloneService) {
        this.seriesService = seriesService;
        this.cloneService = cloneService;
    }

    @CaptureStats(group = "grpc", type = "series", operation = "find", javaClass = GrpcSeriesService.class, javaMethod = "findSeries")
    @Override
    public void findSeries(SeriesServiceProto.FindSeriesRequest request, StreamObserver<SeriesServiceProto.FindSeriesResponse> responseObserver) {
        SeriesServiceProto.FindSeriesResponse.Builder builder = SeriesServiceProto.FindSeriesResponse.newBuilder();
        try {
            builder.addAllSeries(seriesService.findSeries(SERVICE_MAPPER.fromProto(request.getCriteria())).stream()
                .map(MAPPER::toProto)
                .toList());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error finding series", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "create", javaClass = GrpcSeriesService.class, javaMethod = "createSeries")
    @Override
    public void createSeries(SeriesServiceProto.CreateSeriesRequest request, StreamObserver<SeriesServiceProto.CreateSeriesResponse> responseObserver) {
        SeriesServiceProto.CreateSeriesResponse.Builder builder = SeriesServiceProto.CreateSeriesResponse.newBuilder();
        try {
            seriesService.createSeries(MAPPER.fromProto(request.getSeries()));
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error creating series", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "delete", javaClass = GrpcSeriesService.class, javaMethod = "deleteSeries")
    @Override
    public void deleteSeries(SeriesServiceProto.DeleteSeriesRequest request, StreamObserver<SeriesServiceProto.DeleteSeriesResponse> responseObserver) {
        SeriesServiceProto.DeleteSeriesResponse.Builder builder = SeriesServiceProto.DeleteSeriesResponse.newBuilder();
        try {
            seriesService.deleteSeries(request.getId());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error deleting series", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "series", operation = "clone", javaClass = GrpcSeriesService.class, javaMethod = "cloneSeries")
    @Override
    public void cloneSeries(SeriesServiceProto.CloneSeriesRequest request, StreamObserver<SeriesServiceProto.CloneSeriesResponse> responseObserver) {
        SeriesServiceProto.CloneSeriesResponse.Builder builder = SeriesServiceProto.CloneSeriesResponse.newBuilder();
        try {
            cloneService.cloneSeries(request.getSourceId(), request.getDestinationId());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error cloning series", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "definition", operation = "update", javaClass = GrpcSeriesService.class, javaMethod = "updateDefinition")
    @Override
    public void updateDefinition(SeriesServiceProto.UpdateDefinitionRequest request, StreamObserver<SeriesServiceProto.UpdateDefinitionResponse> responseObserver) {
        SeriesServiceProto.UpdateDefinitionResponse.Builder builder = SeriesServiceProto.UpdateDefinitionResponse.newBuilder();
        try {
            cloneService.updateDefinition(request.getId(), MAPPER.fromProto(request.getDefinition()));
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error updating definition", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc", type = "metadata", operation = "update", javaClass = GrpcSeriesService.class, javaMethod = "updateMetadata")
    @Override
    public void updateMetadata(SeriesServiceProto.UpdateMetadataRequest request, StreamObserver<SeriesServiceProto.UpdateMetadataResponse> responseObserver) {
        SeriesServiceProto.UpdateMetadataResponse.Builder builder = SeriesServiceProto.UpdateMetadataResponse.newBuilder();
        try {
            seriesService.updateMetadata(request.getId(), request.getMetadataMap());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error updating metadata", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

}
