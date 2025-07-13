package org.huebert.iotfsdb.api.grpc.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.CommonMapper;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.PartitionPersistenceServiceProto;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.service.PartitionKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

@ExtendWith(MockitoExtension.class)
class PartitionPersistenceServiceTest {

    @Mock
    private PersistenceAdapter persistenceAdapter;

    @Mock
    private StreamObserver<PartitionPersistenceServiceProto.CreatePartitionResponse> createResponseObserver;

    @Mock
    private StreamObserver<PartitionPersistenceServiceProto.GetPartitionsResponse> getResponseObserver;

    @Mock
    private StreamObserver<PartitionPersistenceServiceProto.ReadPartitionResponse> readResponseObserver;

    @Mock
    private PartitionByteBuffer partitionByteBuffer;

    private PartitionPersistenceService service;
    private final CommonMapper mapper = Mappers.getMapper(CommonMapper.class);

    @BeforeEach
    void setUp() {
        service = new PartitionPersistenceService(persistenceAdapter);
    }

    @Test
    void testCreatePartition_Success() {

        // Arrange
        PartitionPersistenceServiceProto.PartitionKey partitionKey =
            PartitionPersistenceServiceProto.PartitionKey.newBuilder()
                .setSeriesId("test-series")
                .setPartitionId("test-partition")
                .build();

        PartitionPersistenceServiceProto.CreatePartitionRequest request =
            PartitionPersistenceServiceProto.CreatePartitionRequest.newBuilder()
                .setKey(partitionKey)
                .setSize(1024)
                .build();

        // Mock successful creation
        doNothing().when(persistenceAdapter).createPartition(any(PartitionKey.class), anyLong());

        // Act
        service.createPartition(request, createResponseObserver);

        // Assert
        verify(persistenceAdapter).createPartition(
            new PartitionKey("test-series", "test-partition"), 1024L);

        ArgumentCaptor<PartitionPersistenceServiceProto.CreatePartitionResponse> responseCaptor =
            ArgumentCaptor.forClass(PartitionPersistenceServiceProto.CreatePartitionResponse.class);
        verify(createResponseObserver).onNext(responseCaptor.capture());
        verify(createResponseObserver).onCompleted();

        PartitionPersistenceServiceProto.CreatePartitionResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
    }

    @Test
    void testCreatePartition_Exception() {

        // Arrange
        PartitionPersistenceServiceProto.PartitionKey partitionKey =
            PartitionPersistenceServiceProto.PartitionKey.newBuilder()
                .setSeriesId("test-series")
                .setPartitionId("test-partition")
                .build();

        PartitionPersistenceServiceProto.CreatePartitionRequest request =
            PartitionPersistenceServiceProto.CreatePartitionRequest.newBuilder()
                .setKey(partitionKey)
                .setSize(1024)
                .build();

        // Mock exception
        doThrow(new RuntimeException("Test exception")).when(persistenceAdapter)
            .createPartition(any(PartitionKey.class), anyLong());

        // Act
        service.createPartition(request, createResponseObserver);

        // Assert
        verify(persistenceAdapter).createPartition(
            new PartitionKey("test-series", "test-partition"), 1024L);

        ArgumentCaptor<PartitionPersistenceServiceProto.CreatePartitionResponse> responseCaptor =
            ArgumentCaptor.forClass(PartitionPersistenceServiceProto.CreatePartitionResponse.class);
        verify(createResponseObserver).onNext(responseCaptor.capture());
        verify(createResponseObserver).onCompleted();

        PartitionPersistenceServiceProto.CreatePartitionResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testGetPartitions_Success() {

        // Arrange
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        CommonProto.Series series = CommonProto.Series.newBuilder()
            .setDefinition(definition)
            .build();

        PartitionPersistenceServiceProto.GetPartitionsRequest request =
            PartitionPersistenceServiceProto.GetPartitionsRequest.newBuilder()
                .setSeries(series)
                .build();

        // Create test data
        Set<PartitionKey> partitionKeys = new HashSet<>();
        partitionKeys.add(new PartitionKey("test-series", "partition-1"));
        partitionKeys.add(new PartitionKey("test-series", "partition-2"));

        // Mock successful retrieval
        when(persistenceAdapter.getPartitions(any(SeriesFile.class))).thenReturn(partitionKeys);

        // Act
        service.getPartitions(request, getResponseObserver);

        // Assert
        verify(persistenceAdapter).getPartitions(any(SeriesFile.class));

        ArgumentCaptor<PartitionPersistenceServiceProto.GetPartitionsResponse> responseCaptor =
            ArgumentCaptor.forClass(PartitionPersistenceServiceProto.GetPartitionsResponse.class);
        verify(getResponseObserver).onNext(responseCaptor.capture());
        verify(getResponseObserver).onCompleted();

        PartitionPersistenceServiceProto.GetPartitionsResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
        assertEquals(2, response.getPartitionsCount());
    }

    @Test
    void testGetPartitions_Exception() {

        // Arrange
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        CommonProto.Series series = CommonProto.Series.newBuilder()
            .setDefinition(definition)
            .build();

        PartitionPersistenceServiceProto.GetPartitionsRequest request =
            PartitionPersistenceServiceProto.GetPartitionsRequest.newBuilder()
                .setSeries(series)
                .build();

        // Mock exception
        when(persistenceAdapter.getPartitions(any(SeriesFile.class)))
            .thenThrow(new RuntimeException("Test exception"));

        // Act
        service.getPartitions(request, getResponseObserver);

        // Assert
        verify(persistenceAdapter).getPartitions(any(SeriesFile.class));

        ArgumentCaptor<PartitionPersistenceServiceProto.GetPartitionsResponse> responseCaptor =
            ArgumentCaptor.forClass(PartitionPersistenceServiceProto.GetPartitionsResponse.class);
        verify(getResponseObserver).onNext(responseCaptor.capture());
        verify(getResponseObserver).onCompleted();

        PartitionPersistenceServiceProto.GetPartitionsResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testReadPartition_Success() {

        // Arrange
        PartitionPersistenceServiceProto.PartitionKey partitionKey =
            PartitionPersistenceServiceProto.PartitionKey.newBuilder()
                .setSeriesId("test-series")
                .setPartitionId("test-partition")
                .build();

        PartitionPersistenceServiceProto.ReadPartitionRequest request =
            PartitionPersistenceServiceProto.ReadPartitionRequest.newBuilder()
                .setKey(partitionKey)
                .build();

        // Create test data
        ByteBuffer testBuffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});

        // Mock successful read
        when(persistenceAdapter.openPartition(any(PartitionKey.class))).thenReturn(partitionByteBuffer);
        when(partitionByteBuffer.getByteBuffer()).thenReturn(testBuffer);

        // Act
        service.readPartition(request, readResponseObserver);

        // Assert
        verify(persistenceAdapter).openPartition(new PartitionKey("test-series", "test-partition"));
        verify(partitionByteBuffer).getByteBuffer();
        verify(partitionByteBuffer).close();

        ArgumentCaptor<PartitionPersistenceServiceProto.ReadPartitionResponse> responseCaptor =
            ArgumentCaptor.forClass(PartitionPersistenceServiceProto.ReadPartitionResponse.class);
        verify(readResponseObserver).onNext(responseCaptor.capture());
        verify(readResponseObserver).onCompleted();

        PartitionPersistenceServiceProto.ReadPartitionResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
        assertEquals(ByteString.copyFrom(new byte[] {1, 2, 3, 4}), response.getData());
    }

    @Test
    void testReadPartition_ExceptionDuringOpen() {

        // Arrange
        PartitionPersistenceServiceProto.PartitionKey partitionKey =
            PartitionPersistenceServiceProto.PartitionKey.newBuilder()
                .setSeriesId("test-series")
                .setPartitionId("test-partition")
                .build();

        PartitionPersistenceServiceProto.ReadPartitionRequest request =
            PartitionPersistenceServiceProto.ReadPartitionRequest.newBuilder()
                .setKey(partitionKey)
                .build();

        // Mock exception during open
        when(persistenceAdapter.openPartition(any(PartitionKey.class)))
            .thenThrow(new RuntimeException("Test exception"));

        // Act
        service.readPartition(request, readResponseObserver);

        // Assert
        verify(persistenceAdapter).openPartition(new PartitionKey("test-series", "test-partition"));

        ArgumentCaptor<PartitionPersistenceServiceProto.ReadPartitionResponse> responseCaptor =
            ArgumentCaptor.forClass(PartitionPersistenceServiceProto.ReadPartitionResponse.class);
        verify(readResponseObserver).onNext(responseCaptor.capture());
        verify(readResponseObserver).onCompleted();

        PartitionPersistenceServiceProto.ReadPartitionResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testReadPartition_ExceptionDuringRead() {

        // Arrange
        PartitionPersistenceServiceProto.PartitionKey partitionKey =
            PartitionPersistenceServiceProto.PartitionKey.newBuilder()
                .setSeriesId("test-series")
                .setPartitionId("test-partition")
                .build();

        PartitionPersistenceServiceProto.ReadPartitionRequest request =
            PartitionPersistenceServiceProto.ReadPartitionRequest.newBuilder()
                .setKey(partitionKey)
                .build();

        // Mock exception during getByteBuffer
        when(persistenceAdapter.openPartition(any(PartitionKey.class))).thenReturn(partitionByteBuffer);
        when(partitionByteBuffer.getByteBuffer()).thenThrow(new RuntimeException("Test exception"));

        // Act
        service.readPartition(request, readResponseObserver);

        // Assert
        verify(persistenceAdapter).openPartition(new PartitionKey("test-series", "test-partition"));
        verify(partitionByteBuffer).getByteBuffer();
        verify(partitionByteBuffer).close();

        ArgumentCaptor<PartitionPersistenceServiceProto.ReadPartitionResponse> responseCaptor =
            ArgumentCaptor.forClass(PartitionPersistenceServiceProto.ReadPartitionResponse.class);
        verify(readResponseObserver).onNext(responseCaptor.capture());
        verify(readResponseObserver).onCompleted();

        PartitionPersistenceServiceProto.ReadPartitionResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }
}
