package org.huebert.iotfsdb.grpc.service;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.grpc.DataServiceGrpc;
import org.huebert.iotfsdb.grpc.Iotfsdb;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.stereotype.Service;

@Service
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
    public void findData(Iotfsdb.FindDataRequest request, StreamObserver<Iotfsdb.FindDataResponse> responseObserver) {
        super.findData(request, responseObserver);
//        responseObserver.onNext();
        responseObserver.onCompleted();
    }

}
