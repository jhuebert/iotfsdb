package org.huebert.iotfsdb.grpc.service;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.grpc.Iotfsdb;
import org.huebert.iotfsdb.grpc.SeriesServiceGrpc;
import org.huebert.iotfsdb.grpc.mapper.GrpcMappers;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class GrpcSeriesService extends SeriesServiceGrpc.SeriesServiceImplBase {

    private static final GrpcMappers MAPPERS = Mappers.getMapper(GrpcMappers.class);

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
    public void findSeries(Iotfsdb.FindSeriesRequest request, StreamObserver<Iotfsdb.FindSeriesResponse> responseObserver) {
        FindSeriesRequest serviceRequest = MAPPERS.fromGrpc(request);
        List<SeriesFile> serviceResponse = seriesService.findSeries(serviceRequest);
        responseObserver.onNext(MAPPERS.toGrpc(serviceResponse));
        responseObserver.onCompleted();
    }

}
