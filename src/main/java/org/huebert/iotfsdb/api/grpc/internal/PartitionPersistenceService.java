package org.huebert.iotfsdb.api.grpc.internal;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.PartitionPersistenceServiceGrpc;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.PartitionPersistenceServiceProto;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.service.PartitionKey;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;

import java.util.Set;

public class PartitionPersistenceService extends PartitionPersistenceServiceGrpc.PartitionPersistenceServiceImplBase {

    private static final CommonMapper TYPES_MAPPER = Mappers.getMapper(CommonMapper.class);

    private final PersistenceAdapter persistenceAdapter;

    public PartitionPersistenceService(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    private static PartitionKey fromGrpc(PartitionPersistenceServiceProto.PartitionKey key) {
        return new PartitionKey(key.getSeriesId(), key.getPartitionId());
    }

    private static PartitionPersistenceServiceProto.PartitionKey toGrpc(PartitionKey key) {
        return PartitionPersistenceServiceProto.PartitionKey.newBuilder()
            .setSeriesId(key.seriesId())
            .setPartitionId(key.partitionId())
            .build();
    }

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "create", javaClass = PartitionPersistenceService.class, javaMethod = "createPartition")
    @Override
    public void createPartition(PartitionPersistenceServiceProto.CreatePartitionRequest request, StreamObserver<PartitionPersistenceServiceProto.CreatePartitionResponse> responseObserver) {
        persistenceAdapter.createPartition(fromGrpc(request.getKey()), request.getSize());
        responseObserver.onNext(PartitionPersistenceServiceProto.CreatePartitionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "get", javaClass = PartitionPersistenceService.class, javaMethod = "getPartitions")
    @Override
    public void getPartitions(PartitionPersistenceServiceProto.GetPartitionsRequest request, StreamObserver<PartitionPersistenceServiceProto.GetPartitionsResponse> responseObserver) {
        Set<PartitionKey> partitions = persistenceAdapter.getPartitions(TYPES_MAPPER.fromGrpc(request.getSeries()));
        responseObserver.onNext(PartitionPersistenceServiceProto.GetPartitionsResponse.newBuilder()
            .addAllPartitions(partitions.stream().map(PartitionPersistenceService::toGrpc).toList())
            .build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "grpc-internal", type = "partition", operation = "read", javaClass = PartitionPersistenceService.class, javaMethod = "readPartition")
    @Override
    public void readPartition(PartitionPersistenceServiceProto.ReadPartitionRequest request, StreamObserver<PartitionPersistenceServiceProto.ReadPartitionResponse> responseObserver) {
        PartitionByteBuffer partitionByteBuffer = persistenceAdapter.openPartition(fromGrpc(request.getKey()));
        try {
            responseObserver.onNext(PartitionPersistenceServiceProto.ReadPartitionResponse.newBuilder()
                .setData(ByteString.copyFrom(partitionByteBuffer.getByteBuffer()))
                .build());
            responseObserver.onCompleted();
        } finally {
            partitionByteBuffer.close();
        }
    }

}
