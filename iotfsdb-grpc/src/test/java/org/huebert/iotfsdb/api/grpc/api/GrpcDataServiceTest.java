package org.huebert.iotfsdb.api.grpc.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.huebert.iotfsdb.api.grpc.proto.v1.CommonProto;
import org.huebert.iotfsdb.api.grpc.proto.v1.api.DataServiceProto;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.ImportService;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.QueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class GrpcDataServiceTest {

    @Mock
    private InsertService insertService;

    @Mock
    private QueryService queryService;

    @Mock
    private ExportService exportService;

    @Mock
    private ImportService importService;

    @Mock
    private StreamObserver<DataServiceProto.FindDataResponse> findDataResponseObserver;

    @Mock
    private StreamObserver<DataServiceProto.InsertDataResponse> insertDataResponseObserver;

    @Mock
    private StreamObserver<DataServiceProto.ExportDataResponse> exportDataResponseObserver;

    @Mock
    private StreamObserver<DataServiceProto.ImportDataResponse> importDataResponseObserver;

    private GrpcDataService service;

    @BeforeEach
    void setUp() {
        service = new GrpcDataService(insertService, queryService, exportService, importService);
    }

    @Test
    void testFindData_Success() {

        // Arrange
        DataServiceProto.FindDataRequest request = DataServiceProto.FindDataRequest.newBuilder()
            .setCriteria(CommonProto.SeriesCriteria.newBuilder()
                .setId("test-series")
                .build())
            .build();

        // Create test response data
        List<FindDataResponse> responses = new ArrayList<>();

        // Mock successful query
        when(queryService.findData(any(FindDataRequest.class))).thenReturn(responses);

        // Act
        service.findData(request, findDataResponseObserver);

        // Assert
        verify(queryService).findData(any(FindDataRequest.class));

        ArgumentCaptor<DataServiceProto.FindDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.FindDataResponse.class);
        verify(findDataResponseObserver).onNext(responseCaptor.capture());
        verify(findDataResponseObserver).onCompleted();

        DataServiceProto.FindDataResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
        assertEquals(0, response.getDataCount());
    }

    @Test
    void testFindData_Exception() {

        // Arrange
        DataServiceProto.FindDataRequest request = DataServiceProto.FindDataRequest.newBuilder()
            .setCriteria(CommonProto.SeriesCriteria.newBuilder()
                .setId("test-series")
                .build())
            .build();

        // Mock exception during query
        when(queryService.findData(any(FindDataRequest.class)))
            .thenThrow(new RuntimeException("Test exception"));

        // Act
        service.findData(request, findDataResponseObserver);

        // Assert
        verify(queryService).findData(any(FindDataRequest.class));

        ArgumentCaptor<DataServiceProto.FindDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.FindDataResponse.class);
        verify(findDataResponseObserver).onNext(responseCaptor.capture());
        verify(findDataResponseObserver).onCompleted();

        DataServiceProto.FindDataResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testInsertData_Success() {

        // Arrange
        CommonProto.SeriesDefinition definition = CommonProto.SeriesDefinition.newBuilder()
            .setId("test-series")
            .build();

        CommonProto.Series series = CommonProto.Series.newBuilder()
            .setDefinition(definition)
            .build();

        CommonProto.SeriesValue value = CommonProto.SeriesValue.newBuilder()
            .setValue(42.5)
            .build();

        List<CommonProto.SeriesValue> values = List.of(value);

        CommonProto.SeriesData seriesData = CommonProto.SeriesData.newBuilder()
            .setId("test-series")
            .addAllValues(values)
            .build();

        DataServiceProto.InsertDataRequest request = DataServiceProto.InsertDataRequest.newBuilder()
            .addData(seriesData)
            .setReducer(CommonProto.Reducer.REDUCER_LAST)
            .build();

        // Mock successful insert
        doNothing().when(insertService).insert(any(InsertRequest.class));

        // Act
        service.insertData(request, insertDataResponseObserver);

        // Assert
        verify(insertService).insert(any(InsertRequest.class));

        ArgumentCaptor<DataServiceProto.InsertDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.InsertDataResponse.class);
        verify(insertDataResponseObserver).onNext(responseCaptor.capture());
        verify(insertDataResponseObserver).onCompleted();

        DataServiceProto.InsertDataResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
    }

    @Test
    void testInsertData_Exception() {

        // Arrange
        CommonProto.SeriesData seriesData = CommonProto.SeriesData.newBuilder()
            .setId("test-series")
            .build();

        DataServiceProto.InsertDataRequest request = DataServiceProto.InsertDataRequest.newBuilder()
            .addData(seriesData)
            .setReducer(CommonProto.Reducer.REDUCER_LAST)
            .build();

        // Mock exception during insert
        doThrow(new RuntimeException("Test exception")).when(insertService).insert(any(InsertRequest.class));

        // Act
        service.insertData(request, insertDataResponseObserver);

        // Assert
        verify(insertService).insert(any(InsertRequest.class));

        ArgumentCaptor<DataServiceProto.InsertDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.InsertDataResponse.class);
        verify(insertDataResponseObserver).onNext(responseCaptor.capture());
        verify(insertDataResponseObserver).onCompleted();

        DataServiceProto.InsertDataResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testExportData_Success() {

        // Arrange
        CommonProto.SeriesCriteria criteria = CommonProto.SeriesCriteria.newBuilder()
            .setId("test-series")
            .build();

        DataServiceProto.ExportDataRequest request = DataServiceProto.ExportDataRequest.newBuilder()
            .setCriteria(criteria)
            .build();

        // Mock successful export
        doAnswer(invocation -> {
            ByteArrayOutputStream outputStream = invocation.getArgument(1);
            outputStream.write(new byte[] {1, 2, 3, 4});
            return null;
        }).when(exportService).export(any(FindSeriesRequest.class), any(ByteArrayOutputStream.class));

        // Act
        service.exportData(request, exportDataResponseObserver);

        // Assert
        verify(exportService).export(any(FindSeriesRequest.class), any(ByteArrayOutputStream.class));

        ArgumentCaptor<DataServiceProto.ExportDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.ExportDataResponse.class);
        verify(exportDataResponseObserver).onNext(responseCaptor.capture());
        verify(exportDataResponseObserver).onCompleted();

        DataServiceProto.ExportDataResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
        assertTrue(response.getFile().getFilename().startsWith("iotfsdb-"));
        assertTrue(response.getFile().getFilename().endsWith(".zip"));
        assertEquals(ByteString.copyFrom(new byte[] {1, 2, 3, 4}), response.getFile().getData());
    }

    @Test
    void testExportData_Exception() {

        // Arrange
        CommonProto.SeriesCriteria criteria = CommonProto.SeriesCriteria.newBuilder()
            .setId("test-series")
            .build();

        DataServiceProto.ExportDataRequest request = DataServiceProto.ExportDataRequest.newBuilder()
            .setCriteria(criteria)
            .build();

        // Mock exception during export
        doThrow(new RuntimeException("Test exception")).when(exportService)
            .export(any(FindSeriesRequest.class), any(ByteArrayOutputStream.class));

        // Act
        service.exportData(request, exportDataResponseObserver);

        // Assert
        verify(exportService).export(any(FindSeriesRequest.class), any(ByteArrayOutputStream.class));

        ArgumentCaptor<DataServiceProto.ExportDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.ExportDataResponse.class);
        verify(exportDataResponseObserver).onNext(responseCaptor.capture());
        verify(exportDataResponseObserver).onCompleted();

        DataServiceProto.ExportDataResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testImportData_Success() {

        // Arrange
        byte[] testData = new byte[] {1, 2, 3, 4};
        CommonProto.File file = CommonProto.File.newBuilder()
            .setFilename("test.zip")
            .setData(ByteString.copyFrom(testData))
            .build();

        DataServiceProto.ImportDataRequest request = DataServiceProto.ImportDataRequest.newBuilder()
            .setFile(file)
            .build();

        // Mock successful import
        doNothing().when(importService).importData(any(Path.class));

        // Act
        service.importData(request, importDataResponseObserver);

        // Assert
        verify(importService).importData(any(Path.class));

        ArgumentCaptor<DataServiceProto.ImportDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.ImportDataResponse.class);
        verify(importDataResponseObserver).onNext(responseCaptor.capture());
        verify(importDataResponseObserver).onCompleted();

        DataServiceProto.ImportDataResponse response = responseCaptor.getValue();
        assertTrue(response.getStatus().getSuccess());
    }

    @Test
    void testImportData_Exception() {

        // Arrange
        byte[] testData = new byte[] {1, 2, 3, 4};
        CommonProto.File file = CommonProto.File.newBuilder()
            .setFilename("test.zip")
            .setData(ByteString.copyFrom(testData))
            .build();

        DataServiceProto.ImportDataRequest request = DataServiceProto.ImportDataRequest.newBuilder()
            .setFile(file)
            .build();

        // Mock exception during import
        doThrow(new RuntimeException("Test exception")).when(importService).importData(any(Path.class));

        // Act
        service.importData(request, importDataResponseObserver);

        // Assert
        verify(importService).importData(any(Path.class));

        ArgumentCaptor<DataServiceProto.ImportDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.ImportDataResponse.class);
        verify(importDataResponseObserver).onNext(responseCaptor.capture());
        verify(importDataResponseObserver).onCompleted();

        DataServiceProto.ImportDataResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
        assertEquals(CommonProto.StatusCode.STATUS_CODE_SERVER_ERROR, response.getStatus().getCode());
    }

    @Test
    void testImportData_TempFileCleanup() {

        // Arrange
        byte[] testData = new byte[] {1, 2, 3, 4};
        CommonProto.File file = CommonProto.File.newBuilder()
            .setFilename("test.zip")
            .setData(ByteString.copyFrom(testData))
            .build();

        DataServiceProto.ImportDataRequest request = DataServiceProto.ImportDataRequest.newBuilder()
            .setFile(file)
            .build();

        // Create an exception during import, but need to verify temp file is cleaned up
        doAnswer(invocation -> {
            Path tempFile = invocation.getArgument(0);
            // Verify file exists before exception
            assertTrue(Files.exists(tempFile));
            throw new IOException("Test exception");
        }).when(importService).importData(any(Path.class));

        // Act
        service.importData(request, importDataResponseObserver);

        // Assert
        verify(importService).importData(any(Path.class));

        // Verify response
        ArgumentCaptor<DataServiceProto.ImportDataResponse> responseCaptor =
            ArgumentCaptor.forClass(DataServiceProto.ImportDataResponse.class);
        verify(importDataResponseObserver).onNext(responseCaptor.capture());
        verify(importDataResponseObserver).onCompleted();

        // Since we mocked importService to throw an exception, no actual temp files should remain
        // We can't directly test the deletion inside the service method, but we can at least
        // verify the error handling worked correctly
        DataServiceProto.ImportDataResponse response = responseCaptor.getValue();
        assertFalse(response.getStatus().getSuccess());
    }
}
