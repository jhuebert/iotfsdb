package org.huebert.iotfsdb.api.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.api.ui.service.BasePageService;
import org.huebert.iotfsdb.api.ui.service.ObjectEncoder;
import org.huebert.iotfsdb.service.SeriesService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.ui.Model;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class MutatingSeriesUiControllerTest {

    @Mock
    private SeriesService seriesService;

    @Mock
    private ObjectEncoder objectEncoder;

    @Mock
    private BasePageService basePageService;

    @Mock
    private Model model;

    @Mock
    private HttpServletResponse response;

    @InjectMocks
    private MutatingSeriesUiController controller;

    private SeriesFile testSeriesFile;
    private BasePageService.BasePage basePage;

    @BeforeEach
    void setUp() {

        // Set up test data
        SeriesDefinition definition = SeriesDefinition.builder()
            .id("test-series")
            .type(NumberType.FLOAT8)
            .interval(60000L)
            .partition(PartitionPeriod.DAY)
            .build();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

        testSeriesFile = SeriesFile.builder()
            .definition(definition)
            .metadata(metadata)
            .build();
    }

    @Test
    void testDeleteSeries() {
        // Act
        controller.deleteSeries("test-series");

        // Assert
        verify(seriesService).deleteSeries("test-series");
    }

    @Test
    void testDeleteMetadata() {
        // Arrange
        when(seriesService.findSeries("test-series")).thenReturn(Optional.of(testSeriesFile));

        // Act
        controller.deleteMetadata("test-series", "key1");

        // Assert
        ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(seriesService).updateMetadata(eq("test-series"), metadataCaptor.capture(), eq(false));

        Map<String, String> updatedMetadata = metadataCaptor.getValue();
        assertFalse(updatedMetadata.containsKey("key1"));
        assertTrue(updatedMetadata.containsKey("key2"));
        assertEquals("value2", updatedMetadata.get("key2"));
    }

    @Test
    void testAddMetadata() {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Arrange
        when(seriesService.findSeries("test-series")).thenReturn(Optional.of(testSeriesFile));

        // Act
        String viewName = controller.addMetadata(model, "test-series", "key3", "value3");

        // Assert
        assertEquals("series/fragments/metadata-row", viewName);

        ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(seriesService).updateMetadata(eq("test-series"), metadataCaptor.capture(), eq(false));

        Map<String, String> updatedMetadata = metadataCaptor.getValue();
        assertTrue(updatedMetadata.containsKey("key1"));
        assertTrue(updatedMetadata.containsKey("key2"));
        assertTrue(updatedMetadata.containsKey("key3"));
        assertEquals("value3", updatedMetadata.get("key3"));

        verify(model).addAttribute("file", testSeriesFile);
        verify(model).addAttribute("key", "key3");
        verify(model).addAttribute("value", "value3");
        verify(model).addAttribute("basePage", basePage);
    }

    @Test
    void testUpdateMetadata() {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Arrange
        when(seriesService.findSeries("test-series")).thenReturn(Optional.of(testSeriesFile));

        // Act
        String viewName = controller.updateMetadata(model, "test-series", "key1", "updated-value");

        // Assert
        assertEquals("series/fragments/metadata-row", viewName);

        ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(seriesService).updateMetadata(eq("test-series"), metadataCaptor.capture(), eq(false));

        Map<String, String> updatedMetadata = metadataCaptor.getValue();
        assertTrue(updatedMetadata.containsKey("key1"));
        assertEquals("updated-value", updatedMetadata.get("key1"));

        verify(model).addAttribute("file", testSeriesFile);
        verify(model).addAttribute("key", "key1");
        verify(model).addAttribute("value", "updated-value");
        verify(model).addAttribute("basePage", basePage);
    }

    @Test
    void testGetCreateSeriesForm() {
        // Act
        String viewName = controller.getCreateSeriesForm(model);

        // Assert
        assertEquals("series/fragments/create", viewName);
    }

    @Test
    void testCreateSeries() throws IOException {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Arrange
        String id = "new-series";
        NumberType type = NumberType.FLOAT4;
        Long interval = 30000L;
        PartitionPeriod partition = PartitionPeriod.MONTH;
        Double min = 0.0;
        Double max = 100.0;

        when(objectEncoder.encode(any(FindSeriesRequest.class))).thenReturn("encoded-request");

        // Act
        String viewName = controller.createSeries(model, response, id, type, interval, partition, min, max);

        // Assert
        assertEquals("series/fragments/search", viewName);

        // Verify series creation
        ArgumentCaptor<SeriesFile> seriesCaptor = ArgumentCaptor.forClass(SeriesFile.class);
        verify(seriesService).createSeries(seriesCaptor.capture());

        SeriesFile createdSeries = seriesCaptor.getValue();
        assertEquals(id, createdSeries.getId());
        assertEquals(type, createdSeries.getDefinition().getType());
        assertEquals(interval, createdSeries.getDefinition().getInterval());
        assertEquals(partition, createdSeries.getDefinition().getPartition());
        assertEquals(min, createdSeries.getDefinition().getMin());
        assertEquals(max, createdSeries.getDefinition().getMax());
        assertTrue(createdSeries.getMetadata().isEmpty());

        // Verify model attributes
        verify(model).addAttribute(eq("series"), anyList());
        verify(model).addAttribute("basePage", basePage);

        // Verify response header
        verify(response).addHeader("HX-Push-Url", "/ui/series?request=encoded-request");
    }

    @Test
    void testCreateSeriesWithEncodingFailure() throws IOException {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Arrange
        String id = "new-series";
        NumberType type = NumberType.FLOAT4;
        Long interval = 30000L;
        PartitionPeriod partition = PartitionPeriod.MONTH;

        when(objectEncoder.encode(any(FindSeriesRequest.class))).thenThrow(new IOException("Encoding failed"));

        // Act
        String viewName = controller.createSeries(model, response, id, type, interval, partition, null, null);

        // Assert
        assertEquals("series/fragments/search", viewName);

        // Verify series creation still happened
        verify(seriesService).createSeries(any(SeriesFile.class));

        // Verify model attributes
        verify(model).addAttribute(eq("series"), anyList());
        verify(model).addAttribute("basePage", basePage);

        // Verify no header was added due to encoding failure
        verify(response, never()).addHeader(anyString(), anyString());
    }

    @Test
    void testGetSeriesFound() {
        // Arrange
        when(seriesService.findSeries("test-series")).thenReturn(Optional.of(testSeriesFile));

        // Act - test private method through public methods
        controller.deleteMetadata("test-series", "key1");

        // Assert - if series wasn't found, the method would throw and the test would fail
        verify(seriesService).findSeries("test-series");
    }

    @Test
    void testGetSeriesNotFound() {
        // Arrange
        when(seriesService.findSeries("non-existent-series")).thenReturn(Optional.empty());

        // Act & Assert
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
            controller.deleteMetadata("non-existent-series", "key1"));

        assertEquals(HttpStatus.NOT_FOUND, exception.getStatusCode());
        assertTrue(exception.getReason().contains("non-existent-series"));
    }
}
