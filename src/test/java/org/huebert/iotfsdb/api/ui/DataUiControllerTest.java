package org.huebert.iotfsdb.api.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletResponse;
import org.huebert.iotfsdb.api.schema.DateTimePreset;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.api.ui.service.BasePageService;
import org.huebert.iotfsdb.api.ui.service.ObjectEncoder;
import org.huebert.iotfsdb.api.ui.service.PlotData;
import org.huebert.iotfsdb.service.QueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ui.Model;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.regex.Pattern;

@ExtendWith(MockitoExtension.class)
class DataUiControllerTest {

    @Mock
    private QueryService queryService;

    @Mock
    private ObjectEncoder objectEncoder;

    @Mock
    private BasePageService basePageService;

    @Mock
    private Model model;

    @Mock
    private HttpServletResponse response;

    @InjectMocks
    private DataUiController controller;

    private FindDataRequest testRequest;
    private List<FindDataResponse> testResponses;
    private ZonedDateTime testTime;
    private BasePageService.BasePage basePage;

    @BeforeEach
    void setUp() {
        // Set up test data
        testTime = ZonedDateTime.of(2025, 7, 12, 10, 0, 0, 0, ZoneId.of("UTC"));

        // Create a test request
        testRequest = new FindDataRequest();
        testRequest.setDateTimePreset(DateTimePreset.LAST_24_HOURS);
        testRequest.setSeries(new FindSeriesRequest(Pattern.compile("test.*"), new HashMap<>()));
        testRequest.setInterval(60000L);
        testRequest.setSize(250);
        testRequest.setTimeReducer(Reducer.AVERAGE);

        // Create test responses
        SeriesDefinition definition = SeriesDefinition.builder()
            .id("test-series")
            .build();

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(definition)
            .metadata(new HashMap<>())
            .build();

        List<SeriesData> seriesData = new ArrayList<>();
        seriesData.add(new SeriesData(testTime, 42.0));
        seriesData.add(new SeriesData(testTime.plusHours(1), 43.0));

        FindDataResponse response = new FindDataResponse(seriesFile, seriesData);
        testResponses = List.of(response);

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .readOnly(true)
            .springdocEnabled(false)
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);
    }

    @Test
    void testGetIndexWithoutRequest() {
        // Act
        String viewName = controller.getIndex(model, null);

        // Assert
        assertEquals("data/index", viewName);

        // Verify model attributes
        ArgumentCaptor<FindDataRequest> requestCaptor = ArgumentCaptor.forClass(FindDataRequest.class);
        verify(model).addAttribute(eq("request"), requestCaptor.capture());
        assertEquals(DateTimePreset.LAST_24_HOURS, requestCaptor.getValue().getDateTimePreset());

        ArgumentCaptor<PlotData> plotDataCaptor = ArgumentCaptor.forClass(PlotData.class);
        verify(model).addAttribute(eq("plotData"), plotDataCaptor.capture());
        assertTrue(plotDataCaptor.getValue().getLabels().isEmpty());
        assertTrue(plotDataCaptor.getValue().getData().isEmpty());

        verify(model).addAttribute(eq("basePage"), eq(basePage));
    }

    @Test
    void testGetIndexWithValidRequest() throws IOException {
        // Arrange
        String encodedRequest = "encoded-request";
        when(objectEncoder.decode(encodedRequest, FindDataRequest.class)).thenReturn(Optional.of(testRequest));
        when(queryService.findData(testRequest)).thenReturn(testResponses);

        // Act
        String viewName = controller.getIndex(model, encodedRequest);

        // Assert
        assertEquals("data/index", viewName);

        // Verify model attributes
        verify(model).addAttribute(eq("request"), eq(testRequest));

        ArgumentCaptor<PlotData> plotDataCaptor = ArgumentCaptor.forClass(PlotData.class);
        verify(model).addAttribute(eq("plotData"), plotDataCaptor.capture());

        PlotData capturedPlotData = plotDataCaptor.getValue();
        assertEquals(2, capturedPlotData.getLabels().size());
        assertEquals(testTime, capturedPlotData.getLabels().get(0));
        assertEquals(testTime.plusHours(1), capturedPlotData.getLabels().get(1));

        assertEquals(1, capturedPlotData.getData().size());
        assertEquals("test-series", capturedPlotData.getData().getFirst().getId());
        assertEquals(42.0, capturedPlotData.getData().getFirst().getValues().get(0));
        assertEquals(43.0, capturedPlotData.getData().getFirst().getValues().get(1));

        verify(model).addAttribute(eq("basePage"), eq(basePage));
    }

    @Test
    void testGetIndexWithInvalidRequest() throws IOException {
        // Arrange
        String encodedRequest = "invalid-request";
        when(objectEncoder.decode(encodedRequest, FindDataRequest.class)).thenThrow(new IOException("Invalid request"));

        // Act
        String viewName = controller.getIndex(model, encodedRequest);

        // Assert
        assertEquals("data/index", viewName);

        // Verify model attributes - should fall back to default request
        ArgumentCaptor<FindDataRequest> requestCaptor = ArgumentCaptor.forClass(FindDataRequest.class);
        verify(model).addAttribute(eq("request"), requestCaptor.capture());
        assertEquals(DateTimePreset.LAST_24_HOURS, requestCaptor.getValue().getDateTimePreset());

        ArgumentCaptor<PlotData> plotDataCaptor = ArgumentCaptor.forClass(PlotData.class);
        verify(model).addAttribute(eq("plotData"), plotDataCaptor.capture());
        assertTrue(plotDataCaptor.getValue().getLabels().isEmpty());
        assertTrue(plotDataCaptor.getValue().getData().isEmpty());
    }

    @Test
    void testSearchData() throws IOException {
        // Arrange
        String timezone = "America/Los_Angeles";
        String search = "id:test-series";
        DateTimePreset dateTimePreset = DateTimePreset.LAST_24_HOURS;
        LocalDateTime from = LocalDateTime.of(2025, 7, 11, 10, 0);
        LocalDateTime to = LocalDateTime.of(2025, 7, 12, 10, 0);
        Long interval = 60000L;
        Integer size = 250;
        String includeNull = "on";
        String useBigDecimal = "on";
        String usePrevious = "on";
        Double nullValue = 0.0;
        Reducer timeReducer = Reducer.AVERAGE;
        Reducer seriesReducer = Reducer.SUM;

        when(queryService.findData(any(FindDataRequest.class))).thenReturn(testResponses);
        when(objectEncoder.encode(any(FindDataRequest.class))).thenReturn("encoded-request");

        // Act
        String viewName = controller.searchData(
            model, response, timezone, search, dateTimePreset,
            from, to, interval, size, usePrevious,
            nullValue, timeReducer, seriesReducer
        );

        // Assert
        assertEquals("data/fragments/script", viewName);

        // Verify FindDataRequest construction and query execution
        ArgumentCaptor<FindDataRequest> requestCaptor = ArgumentCaptor.forClass(FindDataRequest.class);
        verify(queryService).findData(requestCaptor.capture());

        FindDataRequest capturedRequest = requestCaptor.getValue();
        assertEquals(TimeZone.getTimeZone(timezone), capturedRequest.getTimezone());
        assertEquals(dateTimePreset, capturedRequest.getDateTimePreset());
        assertNotNull(capturedRequest.getFrom());
        assertNotNull(capturedRequest.getTo());
        assertEquals(interval, capturedRequest.getInterval());
        assertEquals(size, capturedRequest.getSize());
        assertTrue(capturedRequest.isIncludeNull());
        assertFalse(capturedRequest.isUseBigDecimal());
        assertTrue(capturedRequest.isUsePrevious());
        assertEquals(nullValue, capturedRequest.getNullValue());
        assertEquals(timeReducer, capturedRequest.getTimeReducer());
        assertEquals(seriesReducer, capturedRequest.getSeriesReducer());

        // Verify model attributes
        ArgumentCaptor<PlotData> plotDataCaptor = ArgumentCaptor.forClass(PlotData.class);
        verify(model).addAttribute(eq("plotData"), plotDataCaptor.capture());

        PlotData capturedPlotData = plotDataCaptor.getValue();
        assertEquals(2, capturedPlotData.getLabels().size());
        assertEquals(1, capturedPlotData.getData().size());

        // Verify response header
        verify(response).addHeader("HX-Push-Url", "/ui/data?request=encoded-request");

        // Verify base page is added
        verify(model).addAttribute(eq("basePage"), eq(basePage));
    }

    @Test
    void testSearchDataWithNullOptionalParameters() throws IOException {
        // Arrange
        String timezone = "UTC";
        String search = "id:test-series";
        DateTimePreset dateTimePreset = DateTimePreset.LAST_24_HOURS;

        when(queryService.findData(any(FindDataRequest.class))).thenReturn(testResponses);
        when(objectEncoder.encode(any(FindDataRequest.class))).thenReturn("encoded-request");

        // Act - call with null for optional parameters
        String viewName = controller.searchData(
            model, response, timezone, search, dateTimePreset,
            null, null, 60000L, 250, null,
            null, Reducer.AVERAGE, null
        );

        // Assert
        assertEquals("data/fragments/script", viewName);

        // Verify FindDataRequest construction
        ArgumentCaptor<FindDataRequest> requestCaptor = ArgumentCaptor.forClass(FindDataRequest.class);
        verify(queryService).findData(requestCaptor.capture());

        FindDataRequest capturedRequest = requestCaptor.getValue();
        assertEquals(TimeZone.getTimeZone(timezone), capturedRequest.getTimezone());
        assertEquals(dateTimePreset, capturedRequest.getDateTimePreset());
        assertNull(capturedRequest.getFrom());
        assertNull(capturedRequest.getTo());
        assertEquals(60000L, capturedRequest.getInterval());
        assertEquals(250, capturedRequest.getSize());
        assertTrue(capturedRequest.isIncludeNull());
        assertFalse(capturedRequest.isUseBigDecimal());
        assertFalse(capturedRequest.isUsePrevious());
        assertNull(capturedRequest.getNullValue());
        assertEquals(Reducer.AVERAGE, capturedRequest.getTimeReducer());
        assertNull(capturedRequest.getSeriesReducer());
    }

    @Test
    void testSearchDataWithEncodingFailure() throws IOException {
        // Arrange
        when(queryService.findData(any(FindDataRequest.class))).thenReturn(testResponses);
        when(objectEncoder.encode(any(FindDataRequest.class))).thenThrow(new IOException("Encoding failed"));

        // Act
        String viewName = controller.searchData(
            model, response, "UTC", "id:test-series", DateTimePreset.LAST_24_HOURS,
            null, null, 60000L, 250, null,
            null, Reducer.AVERAGE, null
        );

        // Assert
        assertEquals("data/fragments/script", viewName);

        // Verify header was not added (should not throw exception)
        verify(response, never()).addHeader(anyString(), anyString());

        // Verify model attributes were still set correctly
        verify(model).addAttribute(eq("plotData"), any(PlotData.class));
        verify(model).addAttribute(eq("basePage"), eq(basePage));
    }

    @Test
    void testGetLabelsWithEmptyData() {
        // Test private method through its use in public methods
        // Call getIndex with empty data
        when(queryService.findData(any(FindDataRequest.class))).thenReturn(List.of());

        controller.searchData(model, response, "UTC", "id:test-series", DateTimePreset.LAST_24_HOURS,
            null, null, 60000L, 250, null,
            null, Reducer.AVERAGE, null);

        // Verify empty labels list
        ArgumentCaptor<PlotData> plotDataCaptor = ArgumentCaptor.forClass(PlotData.class);
        verify(model).addAttribute(eq("plotData"), plotDataCaptor.capture());

        assertTrue(plotDataCaptor.getValue().getLabels().isEmpty());
    }

    @Test
    void testGetDataWithNullValues() {
        // Create test response with null values
        SeriesDefinition definition = SeriesDefinition.builder()
            .id("test-series")
            .build();

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(definition)
            .metadata(new HashMap<>())
            .build();

        List<SeriesData> seriesData = new ArrayList<>();
        seriesData.add(new SeriesData(testTime, null));
        seriesData.add(new SeriesData(testTime.plusHours(1), 43.0));

        FindDataResponse response = new FindDataResponse(seriesFile, seriesData);
        List<FindDataResponse> responsesWithNull = List.of(response);

        // Set up the test
        when(queryService.findData(any(FindDataRequest.class))).thenReturn(responsesWithNull);

        // Call searchData
        controller.searchData(
            model, this.response, "UTC", "id:test-series", DateTimePreset.LAST_24_HOURS,
            null, null, 60000L, 250, null,
            null, Reducer.AVERAGE, null
        );

        // Verify null values are handled correctly
        ArgumentCaptor<PlotData> plotDataCaptor = ArgumentCaptor.forClass(PlotData.class);
        verify(model).addAttribute(eq("plotData"), plotDataCaptor.capture());

        PlotData capturedPlotData = plotDataCaptor.getValue();
        assertEquals(1, capturedPlotData.getData().size());
        List<Double> values = capturedPlotData.getData().getFirst().getValues();
        assertEquals(2, values.size());
        assertNull(values.get(0)); // First value should be null
        assertEquals(43.0, values.get(1)); // Second value should be 43.0
    }
}
