package org.huebert.iotfsdb.api.grpc.service;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.api.proto.MetadataServiceGrpc;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class GrpcMetadataService extends MetadataServiceGrpc.MetadataServiceImplBase {

    private final SeriesService seriesService;

    public GrpcMetadataService(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @Override
    public void getMetadata(IotfsdbServices.GetMetadataRequest request, StreamObserver<IotfsdbServices.GetMetadataResponse> responseObserver) {
        seriesService.findSeries(request.getId())
            .map(sf -> IotfsdbServices.GetMetadataResponse.newBuilder()
                .putAllMetadata(sf.getMetadata())
                .build())
            .ifPresent(responseObserver::onNext);

        responseObserver.onCompleted();
    }

    @Override
    public void updateMetadata(IotfsdbServices.UpdateMetadataRequest request, StreamObserver<IotfsdbServices.UpdateMetadataResponse> responseObserver) {
        seriesService.updateMetadata(request.getId(), request.getMetadataMap());
        responseObserver.onNext(IotfsdbServices.UpdateMetadataResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
