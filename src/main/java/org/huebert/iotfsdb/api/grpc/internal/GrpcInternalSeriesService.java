package org.huebert.iotfsdb.api.grpc.internal;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoServicesMapper;
import org.huebert.iotfsdb.api.proto.internal.IotfsdbInternal;
import org.huebert.iotfsdb.api.proto.internal.SeriesServiceGrpc;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class GrpcInternalSeriesService extends SeriesServiceGrpc.SeriesServiceImplBase {

    private static final ProtoServicesMapper MAPPER = Mappers.getMapper(ProtoServicesMapper.class);

    private final PersistenceAdapter persistenceAdapter;

    public GrpcInternalSeriesService(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "delete", javaClass = GrpcInternalSeriesService.class, javaMethod = "deleteSeries")
    @Override
    public void deleteSeries(IotfsdbInternal.DeleteSeriesRequest request, StreamObserver<IotfsdbInternal.DeleteSeriesResponse> responseObserver) {
        super.deleteSeries(request, responseObserver);
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "get", javaClass = GrpcInternalSeriesService.class, javaMethod = "getSeries")
    @Override
    public void getSeries(IotfsdbInternal.GetSeriesRequest request, StreamObserver<IotfsdbInternal.GetSeriesResponse> responseObserver) {
        super.getSeries(request, responseObserver);
    }

    @CaptureStats(group = "grpc-internal", type = "series", operation = "save", javaClass = GrpcInternalSeriesService.class, javaMethod = "saveSeries")
    @Override
    public void saveSeries(IotfsdbInternal.SaveSeriesRequest request, StreamObserver<IotfsdbInternal.SaveSeriesResponse> responseObserver) {
        super.saveSeries(request, responseObserver);
    }
}
