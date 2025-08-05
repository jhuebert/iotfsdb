package org.huebert.iotfsdb.api.grpc.internal;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.PartitionPersistenceServiceGrpc;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.PartitionPersistenceServiceProto;
import org.huebert.iotfsdb.api.schema.PartitionKey;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.mapstruct.factory.Mappers;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.grpc.server.service.GrpcService;

import java.util.Set;

@Slf4j
@GrpcService
@ConditionalOnExpression("${iotfsdb.api.internal:true}")
public class PartitionPersistenceService extends PartitionPersistenceServiceGrpc.PartitionPersistenceServiceImplBase {

    private static final CommonMapper MAPPER = Mappers.getMapper(CommonMapper.class);

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

    @CaptureStats(group = "internal", type = "partition", operation = "create", javaClass = PartitionPersistenceService.class, javaMethod = "createPartition")
    @Override
    public void createPartition(PartitionPersistenceServiceProto.CreatePartitionRequest request, StreamObserver<PartitionPersistenceServiceProto.CreatePartitionResponse> responseObserver) {
        PartitionPersistenceServiceProto.CreatePartitionResponse.Builder builder = PartitionPersistenceServiceProto.CreatePartitionResponse.newBuilder();
        try {
            persistenceAdapter.createPartition(fromGrpc(request.getKey()), request.getSize());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error creating partition", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "internal", type = "partition", operation = "get", javaClass = PartitionPersistenceService.class, javaMethod = "getPartitions")
    @Override
    public void getPartitions(PartitionPersistenceServiceProto.GetPartitionsRequest request, StreamObserver<PartitionPersistenceServiceProto.GetPartitionsResponse> responseObserver) {
        PartitionPersistenceServiceProto.GetPartitionsResponse.Builder builder = PartitionPersistenceServiceProto.GetPartitionsResponse.newBuilder();
        try {
            Set<PartitionKey> partitions = persistenceAdapter.getPartitions(MAPPER.fromProto(request.getSeries()));
            builder.addAllPartitions(partitions.stream().map(PartitionPersistenceService::toGrpc).toList());
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error getting partitions", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "internal", type = "partition", operation = "read", javaClass = PartitionPersistenceService.class, javaMethod = "readPartition")
    @Override
    public void readPartition(PartitionPersistenceServiceProto.ReadPartitionRequest request, StreamObserver<PartitionPersistenceServiceProto.ReadPartitionResponse> responseObserver) {
        PartitionPersistenceServiceProto.ReadPartitionResponse.Builder builder = PartitionPersistenceServiceProto.ReadPartitionResponse.newBuilder();
        try {
            PartitionByteBuffer partitionByteBuffer = persistenceAdapter.openPartition(fromGrpc(request.getKey()));
            try {
                builder.setData(ByteString.copyFrom(partitionByteBuffer.getByteBuffer()));
            } finally {
                partitionByteBuffer.close();
            }
            builder.setStatus(CommonMapper.SUCCESS_STATUS);
        } catch (Exception e) {
            log.error("Error reading partition", e);
            builder.setStatus(MAPPER.getFailedStatus(e));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @CaptureStats(group = "internal", type = "partition", operation = "delete", javaClass = PartitionPersistenceService.class, javaMethod = "deletePartition")
    @Override
    public void deletePartition(PartitionPersistenceServiceProto.DeletePartitionRequest request, StreamObserver<PartitionPersistenceServiceProto.DeletePartitionResponse> responseObserver) {
        // TODO : Implement delete logic
        super.deletePartition(request, responseObserver);
    }

    @CaptureStats(group = "internal", type = "partition", operation = "update", javaClass = PartitionPersistenceService.class, javaMethod = "updatePartition")
    @Override
    public void updatePartition(PartitionPersistenceServiceProto.UpdatePartitionRequest request, StreamObserver<PartitionPersistenceServiceProto.UpdatePartitionResponse> responseObserver) {
        // TODO : Implement update logic
        super.updatePartition(request, responseObserver);
    }

}
