package org.huebert.iotfsdb.api.grpc.internal;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.SeriesPersistenceServiceGrpc;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.SeriesPersistenceServiceProto;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.grpc.server.service.GrpcService;

import java.util.List;

@GrpcService
public class SeriesPersistenceService extends SeriesPersistenceServiceGrpc.SeriesPersistenceServiceImplBase {

    private static final CommonMapper TYPES_MAPPER = Mappers.getMapper(CommonMapper.class);

    private final PersistenceAdapter persistenceAdapter;

    public SeriesPersistenceService(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "delete", javaClass = SeriesPersistenceService.class, javaMethod = "deleteSeries")
    @Override
    public void deleteSeries(SeriesPersistenceServiceProto.DeleteSeriesRequest request, StreamObserver<SeriesPersistenceServiceProto.DeleteSeriesResponse> responseObserver) {
        persistenceAdapter.deleteSeries(request.getId());
        responseObserver.onNext(SeriesPersistenceServiceProto.DeleteSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "get", javaClass = SeriesPersistenceService.class, javaMethod = "getSeries")
    @Override
    public void getSeries(SeriesPersistenceServiceProto.GetSeriesRequest request, StreamObserver<SeriesPersistenceServiceProto.GetSeriesResponse> responseObserver) {
        List<SeriesFile> series = persistenceAdapter.getSeries();
        responseObserver.onNext(SeriesPersistenceServiceProto.GetSeriesResponse.newBuilder()
            .addAllSeries(TYPES_MAPPER.toGrpc(series))
            .build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "save", javaClass = SeriesPersistenceService.class, javaMethod = "saveSeries")
    @Override
    public void saveSeries(SeriesPersistenceServiceProto.SaveSeriesRequest request, StreamObserver<SeriesPersistenceServiceProto.SaveSeriesResponse> responseObserver) {
        persistenceAdapter.saveSeries(TYPES_MAPPER.fromGrpc(request.getSeries()));
        responseObserver.onNext(SeriesPersistenceServiceProto.SaveSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
