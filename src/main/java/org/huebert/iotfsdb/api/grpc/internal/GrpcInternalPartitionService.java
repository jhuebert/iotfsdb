package org.huebert.iotfsdb.api.grpc.internal;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.proto.internal.IotfsdbInternal;
import org.huebert.iotfsdb.api.proto.internal.PartitionServiceGrpc;
import org.huebert.iotfsdb.stats.CaptureStats;

public class GrpcInternalPartitionService extends PartitionServiceGrpc.PartitionServiceImplBase {

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "create", javaClass = GrpcInternalPartitionService.class, javaMethod = "createPartition")
    @Override
    public void createPartition(IotfsdbInternal.CreatePartitionRequest request, StreamObserver<IotfsdbInternal.CreatePartitionResponse> responseObserver) {
        super.createPartition(request, responseObserver);
    }

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "get", javaClass = GrpcInternalPartitionService.class, javaMethod = "getPartitions")
    @Override
    public void getPartitions(IotfsdbInternal.GetPartitionsRequest request, StreamObserver<IotfsdbInternal.GetPartitionsResponse> responseObserver) {
        super.getPartitions(request, responseObserver);
    }

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "read", javaClass = GrpcInternalPartitionService.class, javaMethod = "readPartition")
    @Override
    public void readPartition(IotfsdbInternal.ReadPartitionRequest request, StreamObserver<IotfsdbInternal.ReadPartitionResponse> responseObserver) {
        super.readPartition(request, responseObserver);
    }

}
