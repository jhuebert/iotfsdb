package org.huebert.iotfsdb.api.grpc.service;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.proto.DataServiceGrpc;
import org.huebert.iotfsdb.api.proto.IotfsdbServices;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class GrpcDataService extends DataServiceGrpc.DataServiceImplBase {

    @CaptureStats(
        id = "grpc-data-find",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "grpc"),
            @CaptureStats.Metadata(key = "type", value = "data"),
            @CaptureStats.Metadata(key = "operation", value = "find"),
        }
    )
    @Override
    public void findData(IotfsdbServices.FindDataRequest request, StreamObserver<IotfsdbServices.FindDataResponse> responseObserver) {
        super.findData(request, responseObserver);
//        responseObserver.onNext();
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<IotfsdbServices.InsertDataRequest> insertData(StreamObserver<IotfsdbServices.InsertDataResponse> responseObserver) {
        return super.insertData(responseObserver);
    }

}
