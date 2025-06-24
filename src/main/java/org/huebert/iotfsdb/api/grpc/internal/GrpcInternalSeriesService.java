package org.huebert.iotfsdb.api.grpc.internal;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoInternalMapper;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoTypesMapper;
import org.huebert.iotfsdb.api.proto.internal.IotfsdbInternal;
import org.huebert.iotfsdb.api.proto.internal.SeriesServiceGrpc;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.grpc.server.service.GrpcService;

import java.util.List;

@GrpcService
public class GrpcInternalSeriesService extends SeriesServiceGrpc.SeriesServiceImplBase {

    private static final ProtoInternalMapper INTERNAL_MAPPER = Mappers.getMapper(ProtoInternalMapper.class);

    private static final ProtoTypesMapper TYPES_MAPPER = Mappers.getMapper(ProtoTypesMapper.class);

    private final PersistenceAdapter persistenceAdapter;

    public GrpcInternalSeriesService(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "delete", javaClass = GrpcInternalSeriesService.class, javaMethod = "deleteSeries")
    @Override
    public void deleteSeries(IotfsdbInternal.DeleteSeriesRequest request, StreamObserver<IotfsdbInternal.DeleteSeriesResponse> responseObserver) {
        persistenceAdapter.deleteSeries(request.getId());
        responseObserver.onNext(IotfsdbInternal.DeleteSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "get", javaClass = GrpcInternalSeriesService.class, javaMethod = "getSeries")
    @Override
    public void getSeries(IotfsdbInternal.GetSeriesRequest request, StreamObserver<IotfsdbInternal.GetSeriesResponse> responseObserver) {
        List<SeriesFile> series = persistenceAdapter.getSeries();
        responseObserver.onNext(IotfsdbInternal.GetSeriesResponse.newBuilder()
            .addAllSeries(TYPES_MAPPER.toGrpc(series))
            .build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "save", javaClass = GrpcInternalSeriesService.class, javaMethod = "saveSeries")
    @Override
    public void saveSeries(IotfsdbInternal.SaveSeriesRequest request, StreamObserver<IotfsdbInternal.SaveSeriesResponse> responseObserver) {
        persistenceAdapter.saveSeries(TYPES_MAPPER.fromGrpc(request.getSeries()));
        responseObserver.onNext(IotfsdbInternal.SaveSeriesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
