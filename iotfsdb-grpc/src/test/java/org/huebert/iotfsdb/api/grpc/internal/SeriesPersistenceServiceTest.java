package org.huebert.iotfsdb.api.grpc.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.grpc.proto.v1.internal.SeriesPersistenceServiceProto;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class SeriesPersistenceServiceTest {

    @Mock
    private PersistenceAdapter persistenceAdapter;

    @Mock
    private StreamObserver<SeriesPersistenceServiceProto.DeleteSeriesResponse> deleteResponseObserver;

    @Mock
    private StreamObserver<SeriesPersistenceServiceProto.GetSeriesResponse> getResponseObserver;

    @Mock
    private StreamObserver<SeriesPersistenceServiceProto.SaveSeriesResponse> saveResponseObserver;

    private SeriesPersistenceService service;

    @BeforeEach
    void setUp() {
        service = new SeriesPersistenceService(persistenceAdapter);
    }

    @Test
    void testDeleteSeries_Success() {

        // Arrange
        SeriesPersistenceServiceProto.DeleteSeriesRequest request =
            SeriesPersistenceServiceProto.DeleteSeriesRequest.newBuilder()
                .setId("test-series-id")
                .build();

        // Mock successful deletion
        doNothing().when(persistenceAdapter).deleteSeries(anyString());

        // Act
        service.deleteSeries(request, deleteResponseObserver);

        // Assert
        verify(persistenceAdapter).deleteSeries("test-series-id");

        ArgumentCaptor<SeriesPersistenceServiceProto.DeleteSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesPersistenceServiceProto.DeleteSeriesResponse.class);
        verify(deleteResponseObserver).onNext(responseCaptor.capture());
        verify(deleteResponseObserver).onCompleted();

        SeriesPersistenceServiceProto.DeleteSeriesResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_UNSPECIFIED, response.getStatus().getCode());
    }

    @Test
    void testDeleteSeries_Exception() {
        // Arrange
        SeriesPersistenceServiceProto.DeleteSeriesRequest request =
            SeriesPersistenceServiceProto.DeleteSeriesRequest.newBuilder()
                .setId("test-series-id")
                .build();

        // Mock exception
        doThrow(new RuntimeException("Test exception")).when(persistenceAdapter).deleteSeries(anyString());

        // Act
        service.deleteSeries(request, deleteResponseObserver);

        // Assert
        verify(persistenceAdapter).deleteSeries("test-series-id");

        ArgumentCaptor<SeriesPersistenceServiceProto.DeleteSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesPersistenceServiceProto.DeleteSeriesResponse.class);
        verify(deleteResponseObserver).onNext(responseCaptor.capture());
        verify(deleteResponseObserver).onCompleted();

        SeriesPersistenceServiceProto.DeleteSeriesResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
        assertEquals("Error processing request", response.getStatus().getMessage());
    }

    @Test
    void testGetSeries_Success() {
        // Arrange
        SeriesPersistenceServiceProto.GetSeriesRequest request =
            SeriesPersistenceServiceProto.GetSeriesRequest.newBuilder().build();

        // Create test data
        SeriesFile testSeries = SeriesFile.builder()
            .definition(null) // Simplified for test
            .metadata(Collections.emptyMap())
            .build();

        // Mock successful retrieval
        when(persistenceAdapter.getSeries()).thenReturn(List.of(testSeries));

        // Act
        service.getSeries(request, getResponseObserver);

        // Assert
        verify(persistenceAdapter).getSeries();

        ArgumentCaptor<SeriesPersistenceServiceProto.GetSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesPersistenceServiceProto.GetSeriesResponse.class);
        verify(getResponseObserver).onNext(responseCaptor.capture());
        verify(getResponseObserver).onCompleted();

        SeriesPersistenceServiceProto.GetSeriesResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
        assertEquals(1, response.getSeriesCount());
    }

    @Test
    void testGetSeries_Exception() {
        // Arrange
        SeriesPersistenceServiceProto.GetSeriesRequest request =
            SeriesPersistenceServiceProto.GetSeriesRequest.newBuilder().build();

        // Mock exception
        when(persistenceAdapter.getSeries()).thenThrow(new RuntimeException("Test exception"));

        // Act
        service.getSeries(request, getResponseObserver);

        // Assert
        verify(persistenceAdapter).getSeries();

        ArgumentCaptor<SeriesPersistenceServiceProto.GetSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesPersistenceServiceProto.GetSeriesResponse.class);
        verify(getResponseObserver).onNext(responseCaptor.capture());
        verify(getResponseObserver).onCompleted();

        SeriesPersistenceServiceProto.GetSeriesResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
        assertEquals("Error processing request", response.getStatus().getMessage());
    }

    @Test
    void testSaveSeries_Success() {
        // Arrange
        // Create a protobuf Series object
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        CommonProto.Series series = CommonProto.Series.newBuilder()
            .setDefinition(definition)
            .build();

        SeriesPersistenceServiceProto.SaveSeriesRequest request =
            SeriesPersistenceServiceProto.SaveSeriesRequest.newBuilder()
                .setSeries(series)
                .build();

        // Mock successful save
        doNothing().when(persistenceAdapter).saveSeries(any(SeriesFile.class));

        // Act
        service.saveSeries(request, saveResponseObserver);

        // Assert
        verify(persistenceAdapter).saveSeries(any(SeriesFile.class));

        ArgumentCaptor<SeriesPersistenceServiceProto.SaveSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesPersistenceServiceProto.SaveSeriesResponse.class);
        verify(saveResponseObserver).onNext(responseCaptor.capture());
        verify(saveResponseObserver).onCompleted();

        SeriesPersistenceServiceProto.SaveSeriesResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_UNSPECIFIED, response.getStatus().getCode());
    }

    @Test
    void testSaveSeries_Exception() {
        // Arrange
        // Create a protobuf Series object
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        CommonProto.Series series = CommonProto.Series.newBuilder()
            .setDefinition(definition)
            .build();

        SeriesPersistenceServiceProto.SaveSeriesRequest request =
            SeriesPersistenceServiceProto.SaveSeriesRequest.newBuilder()
                .setSeries(series)
                .build();

        // Mock exception
        doThrow(new RuntimeException("Test exception")).when(persistenceAdapter).saveSeries(any(SeriesFile.class));

        // Act
        service.saveSeries(request, saveResponseObserver);

        // Assert
        verify(persistenceAdapter).saveSeries(any(SeriesFile.class));

        ArgumentCaptor<SeriesPersistenceServiceProto.SaveSeriesResponse> responseCaptor =
            ArgumentCaptor.forClass(SeriesPersistenceServiceProto.SaveSeriesResponse.class);
        verify(saveResponseObserver).onNext(responseCaptor.capture());
        verify(saveResponseObserver).onCompleted();

        SeriesPersistenceServiceProto.SaveSeriesResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
        assertEquals("Error processing request", response.getStatus().getMessage());
    }
}
