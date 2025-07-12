package org.huebert.iotfsdb.api.grpc.internal;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.SeriesPersistenceServiceGrpc;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.SeriesPersistenceServiceProto;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
@ConditionalOnExpression("${iotfsdb.api.internal:true}")
public class SeriesPersistenceService extends SeriesPersistenceServiceGrpc.SeriesPersistenceServiceImplBase {

    private static final CommonMapper MAPPER = Mappers.getMapper(CommonMapper.class);

    private final PersistenceAdapter persistenceAdapter;

    public SeriesPersistenceService(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "delete", javaClass = SeriesPersistenceService.class, javaMethod = "deleteSeries")
    @Override
    public void deleteSeries(SeriesPersistenceServiceProto.DeleteSeriesRequest request, StreamObserver<SeriesPersistenceServiceProto.DeleteSeriesResponse> responseObserver) {
        SeriesPersistenceServiceProto.DeleteSeriesResponse.Builder builder = SeriesPersistenceServiceProto.DeleteSeriesResponse.newBuilder();
        try {
            persistenceAdapter.deleteSeries(request.getId());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error deleting series", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "get", javaClass = SeriesPersistenceService.class, javaMethod = "getSeries")
    @Override
    public void getSeries(SeriesPersistenceServiceProto.GetSeriesRequest request, StreamObserver<SeriesPersistenceServiceProto.GetSeriesResponse> responseObserver) {
        SeriesPersistenceServiceProto.GetSeriesResponse.Builder builder = SeriesPersistenceServiceProto.GetSeriesResponse.newBuilder();
        try {
            builder.addAllSeries(persistenceAdapter.getSeries().stream().map(MAPPER::toProto).toList());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error getting series", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "save", javaClass = SeriesPersistenceService.class, javaMethod = "saveSeries")
    @Override
    public void saveSeries(SeriesPersistenceServiceProto.SaveSeriesRequest request, StreamObserver<SeriesPersistenceServiceProto.SaveSeriesResponse> responseObserver) {
        SeriesPersistenceServiceProto.SaveSeriesResponse.Builder builder = SeriesPersistenceServiceProto.SaveSeriesResponse.newBuilder();
        try {
            persistenceAdapter.saveSeries(MAPPER.fromProto(request.getSeries()));
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error saving series", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
