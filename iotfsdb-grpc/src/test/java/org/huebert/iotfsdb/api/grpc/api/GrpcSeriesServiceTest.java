package org.huebert.iotfsdb.api.grpc.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.SeriesServiceProto;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.CloneService;
import org.huebert.iotfsdb.service.SeriesService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class GrpcSeriesServiceTest {

    @Mock
    private SeriesService seriesService;

    @Mock
    private CloneService cloneService;

    @Mock
    private StreamObserver<SeriesServiceProto.FindSeriesResponse> findSeriesResponseObserver;

    @Mock
    private StreamObserver<SeriesServiceProto.CreateSeriesResponse> createSeriesResponseObserver;

    @Mock
    private StreamObserver<SeriesServiceProto.DeleteSeriesResponse> deleteSeriesResponseObserver;

    @Mock
    private StreamObserver<SeriesServiceProto.CloneSeriesResponse> cloneSeriesResponseObserver;

    @Mock
    private StreamObserver<SeriesServiceProto.UpdateDefinitionResponse> updateDefinitionResponseObserver;

    @Mock
    private StreamObserver<SeriesServiceProto.UpdateMetadataResponse> updateMetadataResponseObserver;

    private GrpcSeriesService service;

    @BeforeEach
    void setUp() {
        service = new GrpcSeriesService(seriesService, cloneService);
    }

    @Test
    void testFindSeries_Success() {

        // Arrange
        CommonProto.SeriesCriteria criteria = CommonProto.SeriesCriteria.newBuilder()
            .setId("test-series")
            .build();

        SeriesServiceProto.FindSeriesRequest request = SeriesServiceProto.FindSeriesRequest.newBuilder()
            .setCriteria(criteria)
            .build();

        // Create test data
        SeriesDefinition definition = SeriesDefinition.builder()
            .id("test-series")
            .build();

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(definition)
            .metadata(new HashMap<>())
            .build();

        List<SeriesFile> seriesFiles = new ArrayList<>();
        seriesFiles.add(seriesFile);

        // Mock successful find
        when(seriesService.findSeries(any(FindSeriesRequest.class))).thenReturn(seriesFiles);

        // Act
        service.findSeries(request, findSeriesResponseObserver);

        // Assert
        verify(seriesService).findSeries(any(FindSeriesRequest.class));

        ArgumentCaptor<SeriesServiceProto.FindSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.FindSeriesResponse.class);
        verify(findSeriesResponseObserver).onNext(responseCaptor.capture());
        verify(findSeriesResponseObserver).onCompleted();

        SeriesServiceProto.FindSeriesResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
        assertEquals(1, response.getSeriesCount());
        assertEquals("test-series", response.getSeries(0).getDefinition().getId());
    }

    @Test
    void testFindSeries_Exception() {

        // Arrange
        CommonProto.SeriesCriteria criteria = CommonProto.SeriesCriteria.newBuilder()
            .setId("test-series")
            .build();

        SeriesServiceProto.FindSeriesRequest request = SeriesServiceProto.FindSeriesRequest.newBuilder()
            .setCriteria(criteria)
            .build();

        // Mock exception
        when(seriesService.findSeries(any(FindSeriesRequest.class)))
            .thenThrow(new RuntimeException("Test exception"));

        // Act
        service.findSeries(request, findSeriesResponseObserver);

        // Assert
        verify(seriesService).findSeries(any(FindSeriesRequest.class));

        ArgumentCaptor<SeriesServiceProto.FindSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.FindSeriesResponse.class);
        verify(findSeriesResponseObserver).onNext(responseCaptor.capture());
        verify(findSeriesResponseObserver).onCompleted();

        SeriesServiceProto.FindSeriesResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testCreateSeries_Success() {

        // Arrange
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        CommonProto.Series series = CommonProto.Series.newBuilder()
            .setDefinition(definition)
            .build();

        SeriesServiceProto.CreateSeriesRequest request = SeriesServiceProto.CreateSeriesRequest.newBuilder()
            .setSeries(series)
            .build();

        // Mock successful creation
        doNothing().when(seriesService).createSeries(any(SeriesFile.class));

        // Act
        service.createSeries(request, createSeriesResponseObserver);

        // Assert
        verify(seriesService).createSeries(any(SeriesFile.class));

        ArgumentCaptor<SeriesServiceProto.CreateSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.CreateSeriesResponse.class);
        verify(createSeriesResponseObserver).onNext(responseCaptor.capture());
        verify(createSeriesResponseObserver).onCompleted();

        SeriesServiceProto.CreateSeriesResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
    }

    @Test
    void testCreateSeries_Exception() {

        // Arrange
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        CommonProto.Series series = CommonProto.Series.newBuilder()
            .setDefinition(definition)
            .build();

        SeriesServiceProto.CreateSeriesRequest request = SeriesServiceProto.CreateSeriesRequest.newBuilder()
            .setSeries(series)
            .build();

        // Mock exception
        doThrow(new RuntimeException("Test exception")).when(seriesService).createSeries(any(SeriesFile.class));

        // Act
        service.createSeries(request, createSeriesResponseObserver);

        // Assert
        verify(seriesService).createSeries(any(SeriesFile.class));

        ArgumentCaptor<SeriesServiceProto.CreateSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.CreateSeriesResponse.class);
        verify(createSeriesResponseObserver).onNext(responseCaptor.capture());
        verify(createSeriesResponseObserver).onCompleted();

        SeriesServiceProto.CreateSeriesResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testDeleteSeries_Success() {

        // Arrange
        SeriesServiceProto.DeleteSeriesRequest request = SeriesServiceProto.DeleteSeriesRequest.newBuilder()
            .setId("test-series")
            .build();

        // Mock successful deletion
        doNothing().when(seriesService).deleteSeries(anyString());

        // Act
        service.deleteSeries(request, deleteSeriesResponseObserver);

        // Assert
        verify(seriesService).deleteSeries("test-series");

        ArgumentCaptor<SeriesServiceProto.DeleteSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.DeleteSeriesResponse.class);
        verify(deleteSeriesResponseObserver).onNext(responseCaptor.capture());
        verify(deleteSeriesResponseObserver).onCompleted();

        SeriesServiceProto.DeleteSeriesResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
    }

    @Test
    void testDeleteSeries_Exception() {

        // Arrange
        SeriesServiceProto.DeleteSeriesRequest request = SeriesServiceProto.DeleteSeriesRequest.newBuilder()
            .setId("test-series")
            .build();

        // Mock exception
        doThrow(new RuntimeException("Test exception")).when(seriesService).deleteSeries(anyString());

        // Act
        service.deleteSeries(request, deleteSeriesResponseObserver);

        // Assert
        verify(seriesService).deleteSeries("test-series");

        ArgumentCaptor<SeriesServiceProto.DeleteSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.DeleteSeriesResponse.class);
        verify(deleteSeriesResponseObserver).onNext(responseCaptor.capture());
        verify(deleteSeriesResponseObserver).onCompleted();

        SeriesServiceProto.DeleteSeriesResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testCloneSeries_Success() {

        // Arrange
        SeriesServiceProto.CloneSeriesRequest request = SeriesServiceProto.CloneSeriesRequest.newBuilder()
            .setSourceId("source-series")
            .setDestinationId("destination-series")
            .setIncludeData(true)
            .build();

        // Mock successful clone
        doNothing().when(cloneService).cloneSeries(anyString(), anyString(), anyBoolean());

        // Act
        service.cloneSeries(request, cloneSeriesResponseObserver);

        // Assert
        verify(cloneService).cloneSeries("source-series", "destination-series", true);

        ArgumentCaptor<SeriesServiceProto.CloneSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.CloneSeriesResponse.class);
        verify(cloneSeriesResponseObserver).onNext(responseCaptor.capture());
        verify(cloneSeriesResponseObserver).onCompleted();

        SeriesServiceProto.CloneSeriesResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
    }

    @Test
    void testCloneSeries_Exception() {

        // Arrange
        SeriesServiceProto.CloneSeriesRequest request = SeriesServiceProto.CloneSeriesRequest.newBuilder()
            .setSourceId("source-series")
            .setDestinationId("destination-series")
            .setIncludeData(true)
            .build();

        // Mock exception
        doThrow(new RuntimeException("Test exception"))
            .when(cloneService).cloneSeries(anyString(), anyString(), anyBoolean());

        // Act
        service.cloneSeries(request, cloneSeriesResponseObserver);

        // Assert
        verify(cloneService).cloneSeries("source-series", "destination-series", true);

        ArgumentCaptor<SeriesServiceProto.CloneSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.CloneSeriesResponse.class);
        verify(cloneSeriesResponseObserver).onNext(responseCaptor.capture());
        verify(cloneSeriesResponseObserver).onCompleted();

        SeriesServiceProto.CloneSeriesResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testUpdateDefinition_Success() {

        // Arrange
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        SeriesServiceProto.UpdateDefinitionRequest request = SeriesServiceProto.UpdateDefinitionRequest.newBuilder()
            .setId("test-series")
            .setDefinition(definition)
            .build();

        // Mock successful update
        doNothing().when(cloneService).updateDefinition(anyString(), any(SeriesDefinition.class));

        // Act
        service.updateDefinition(request, updateDefinitionResponseObserver);

        // Assert
        verify(cloneService).updateDefinition(eq("test-series"), any(SeriesDefinition.class));

        ArgumentCaptor<SeriesServiceProto.UpdateDefinitionResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.UpdateDefinitionResponse.class);
        verify(updateDefinitionResponseObserver).onNext(responseCaptor.capture());
        verify(updateDefinitionResponseObserver).onCompleted();

        SeriesServiceProto.UpdateDefinitionResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
    }

    @Test
    void testUpdateDefinition_Exception() {

        // Arrange
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        SeriesServiceProto.UpdateDefinitionRequest request = SeriesServiceProto.UpdateDefinitionRequest.newBuilder()
            .setId("test-series")
            .setDefinition(definition)
            .build();

        // Mock exception
        doThrow(new RuntimeException("Test exception"))
            .when(cloneService).updateDefinition(anyString(), any(SeriesDefinition.class));

        // Act
        service.updateDefinition(request, updateDefinitionResponseObserver);

        // Assert
        verify(cloneService).updateDefinition(eq("test-series"), any(SeriesDefinition.class));

        ArgumentCaptor<SeriesServiceProto.UpdateDefinitionResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.UpdateDefinitionResponse.class);
        verify(updateDefinitionResponseObserver).onNext(responseCaptor.capture());
        verify(updateDefinitionResponseObserver).onCompleted();

        SeriesServiceProto.UpdateDefinitionResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testUpdateMetadata_Success() {

        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");

        SeriesServiceProto.UpdateMetadataRequest request = SeriesServiceProto.UpdateMetadataRequest.newBuilder()
            .setId("test-series")
            .putAllMetadata(metadata)
            .setMerge(true)
            .build();

        // Mock successful update
        doNothing().when(seriesService).updateMetadata(anyString(), anyMap(), anyBoolean());

        // Act
        service.updateMetadata(request, updateMetadataResponseObserver);

        // Assert
        verify(seriesService).updateMetadata(eq("test-series"), anyMap(), eq(true));

        ArgumentCaptor<SeriesServiceProto.UpdateMetadataResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.UpdateMetadataResponse.class);
        verify(updateMetadataResponseObserver).onNext(responseCaptor.capture());
        verify(updateMetadataResponseObserver).onCompleted();

        SeriesServiceProto.UpdateMetadataResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
    }

    @Test
    void testUpdateMetadata_Exception() {

        // Arrange
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");

        SeriesServiceProto.UpdateMetadataRequest request = SeriesServiceProto.UpdateMetadataRequest.newBuilder()
            .setId("test-series")
            .putAllMetadata(metadata)
            .setMerge(true)
            .build();

        // Mock exception
        doThrow(new RuntimeException("Test exception"))
            .when(seriesService).updateMetadata(anyString(), anyMap(), anyBoolean());

        // Act
        service.updateMetadata(request, updateMetadataResponseObserver);

        // Assert
        verify(seriesService).updateMetadata(eq("test-series"), anyMap(), eq(true));

        ArgumentCaptor<SeriesServiceProto.UpdateMetadataResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesServiceProto.UpdateMetadataResponse.class);
        verify(updateMetadataResponseObserver).onNext(responseCaptor.capture());
        verify(updateMetadataResponseObserver).onCompleted();

        SeriesServiceProto.UpdateMetadataResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }
}
