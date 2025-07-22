package org.huebert.iotfsdb.api.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.api.ui.service.BasePageService;
import org.huebert.iotfsdb.api.ui.service.ExportUiService;
import org.huebert.iotfsdb.api.ui.service.ObjectEncoder;
import org.huebert.iotfsdb.service.SeriesService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

@ExtendWith(MockitoExtension.class)
class SeriesUiControllerTest {

    @Mock
    private SeriesService seriesService;

    @Mock
    private ExportUiService exportService;

    @Mock
    private ObjectEncoder objectEncoder;

    @Mock
    private BasePageService basePageService;

    @Mock
    private Model model;

    @Mock
    private HttpServletResponse response;

    @InjectMocks
    private SeriesUiController controller;

    private List<SeriesFile> testSeriesList;
    private FindSeriesRequest testRequest;
    private BasePageService.BasePage basePage;

    @BeforeEach
    void setUp() {
        // Create test data
        testSeriesList = new ArrayList<>();
        testSeriesList.add(SeriesFile.builder().definition(null).metadata(new HashMap<>()).build());

        testRequest = new FindSeriesRequest(Pattern.compile("test.*"), new HashMap<>());
    }

    @Test
    void testGetIndexWithoutRequest() {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Act
        String viewName = controller.getIndex(model, null);

        // Assert
        assertEquals("series/index", viewName);

        // Verify model attributes
        verify(model).addAttribute(eq("series"), eq(List.of()));
        verify(model).addAttribute(eq("basePage"), eq(basePage));
        verify(model, never()).addAttribute(eq("request"), any());

        // Verify no service interactions
        verify(seriesService, never()).findSeries(any(FindSeriesRequest.class));
    }

    @Test
    void testGetIndexWithValidRequest() throws IOException {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Arrange
        String encodedRequest = "encoded-request";
        when(objectEncoder.decode(encodedRequest, FindSeriesRequest.class)).thenReturn(Optional.of(testRequest));
        when(seriesService.findSeries(testRequest)).thenReturn(testSeriesList);

        // Act
        String viewName = controller.getIndex(model, encodedRequest);

        // Assert
        assertEquals("series/index", viewName);

        // Verify model attributes
        verify(model).addAttribute(eq("series"), eq(testSeriesList));
        verify(model).addAttribute(eq("basePage"), eq(basePage));
        verify(model).addAttribute(eq("request"), eq(testRequest));

        // Verify service interactions
        verify(seriesService).findSeries(testRequest);
    }

    @Test
    void testGetIndexWithInvalidRequest() throws IOException {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Arrange
        String encodedRequest = "invalid-request";
        when(objectEncoder.decode(encodedRequest, FindSeriesRequest.class)).thenThrow(new IOException("Invalid request"));

        // Act
        String viewName = controller.getIndex(model, encodedRequest);

        // Assert
        assertEquals("series/index", viewName);

        // Verify model attributes - should return empty list for series
        verify(model).addAttribute(eq("series"), eq(List.of()));
        verify(model).addAttribute(eq("basePage"), eq(basePage));
        verify(model, never()).addAttribute(eq("request"), any());

        // Verify no service interactions beyond the failed decode
        verify(seriesService, never()).findSeries(any(FindSeriesRequest.class));
    }

    @Test
    void testSearch() throws IOException {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Arrange
        String searchQuery = "id:test-series";
        when(seriesService.findSeries(any(FindSeriesRequest.class))).thenReturn(testSeriesList);
        when(objectEncoder.encode(any(FindSeriesRequest.class))).thenReturn("encoded-request");

        // Act
        String viewName = controller.search(model, response, searchQuery);

        // Assert
        assertEquals("series/fragments/results", viewName);

        // Verify model attributes
        verify(model).addAttribute(eq("series"), eq(testSeriesList));
        verify(model).addAttribute(eq("basePage"), eq(basePage));

        // Verify request parsing and service interaction
        ArgumentCaptor<FindSeriesRequest> requestCaptor = ArgumentCaptor.forClass(FindSeriesRequest.class);
        verify(seriesService).findSeries(requestCaptor.capture());

        FindSeriesRequest capturedRequest = requestCaptor.getValue();
        assertNotNull(capturedRequest);
        assertNotNull(capturedRequest.getPattern());

        // Verify response header
        verify(response).addHeader("HX-Push-Url", "/ui/series?request=encoded-request");
    }

    @Test
    void testSearchWithEncodingFailure() throws IOException {

        // Set up base page
        basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Arrange
        String searchQuery = "id:test-series";
        when(seriesService.findSeries(any(FindSeriesRequest.class))).thenReturn(testSeriesList);
        when(objectEncoder.encode(any(FindSeriesRequest.class))).thenThrow(new IOException("Encoding failed"));

        // Act
        String viewName = controller.search(model, response, searchQuery);

        // Assert
        assertEquals("series/fragments/results", viewName);

        // Verify model attributes
        verify(model).addAttribute(eq("series"), eq(testSeriesList));
        verify(model).addAttribute(eq("basePage"), eq(basePage));

        // Verify request parsing and service interaction still happened
        verify(seriesService).findSeries(any(FindSeriesRequest.class));

        // Verify no header was added (but no exception should have been thrown)
        verify(response, never()).addHeader(anyString(), anyString());
    }

    @Test
    void testExportSeries() {
        // Arrange
        String seriesId = "test-series";

        ResponseEntity<StreamingResponseBody> expectedResponse = ResponseEntity
            .status(HttpStatus.OK)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"test-series.zip\"")
            .body(outputStream -> {
                // Test body that does nothing
            });

        when(exportService.export(seriesId)).thenReturn(expectedResponse);

        // Act
        ResponseEntity<StreamingResponseBody> response = controller.exportSeries(seriesId);

        // Assert
        assertSame(expectedResponse, response);

        // Verify service interaction
        verify(exportService).export(seriesId);
    }
}
