package org.huebert.iotfsdb.api.grpc.internal;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoInternalMapper;
import org.huebert.iotfsdb.api.grpc.mapper.ProtoTypesMapper;
import org.huebert.iotfsdb.api.proto.internal.IotfsdbInternal;
import org.huebert.iotfsdb.api.proto.internal.PartitionServiceGrpc;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.service.PartitionKey;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;

import java.util.Set;

public class GrpcInternalPartitionService extends PartitionServiceGrpc.PartitionServiceImplBase {

    private static final ProtoInternalMapper INTERNAL_MAPPER = Mappers.getMapper(ProtoInternalMapper.class);

    private static final ProtoTypesMapper TYPES_MAPPER = Mappers.getMapper(ProtoTypesMapper.class);

    private final PersistenceAdapter persistenceAdapter;

    public GrpcInternalPartitionService(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "create", javaClass = GrpcInternalPartitionService.class, javaMethod = "createPartition")
    @Override
    public void createPartition(IotfsdbInternal.CreatePartitionRequest request, StreamObserver<IotfsdbInternal.CreatePartitionResponse> responseObserver) {
        persistenceAdapter.createPartition(INTERNAL_MAPPER.fromGrpc(request.getKey()), request.getSize());
        responseObserver.onNext(IotfsdbInternal.CreatePartitionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "get", javaClass = GrpcInternalPartitionService.class, javaMethod = "getPartitions")
    @Override
    public void getPartitions(IotfsdbInternal.GetPartitionsRequest request, StreamObserver<IotfsdbInternal.GetPartitionsResponse> responseObserver) {
        Set<PartitionKey> partitions = persistenceAdapter.getPartitions(TYPES_MAPPER.fromGrpc(request.getSeries()));
        responseObserver.onNext(IotfsdbInternal.GetPartitionsResponse.newBuilder()
            .addAllPartitions(INTERNAL_MAPPER.toPartitionGrpc(partitions))
            .build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "read", javaClass = GrpcInternalPartitionService.class, javaMethod = "readPartition")
    @Override
    public void readPartition(IotfsdbInternal.ReadPartitionRequest request, StreamObserver<IotfsdbInternal.ReadPartitionResponse> responseObserver) {
        PartitionByteBuffer partitionByteBuffer = persistenceAdapter.openPartition(INTERNAL_MAPPER.fromGrpc(request.getKey()));
        try {
            responseObserver.onNext(IotfsdbInternal.ReadPartitionResponse.newBuilder()
                .setData(ByteString.copyFrom(partitionByteBuffer.getByteBuffer()))
                .build());
            responseObserver.onCompleted();
        } finally {
            partitionByteBuffer.close();
        }

    }

}
