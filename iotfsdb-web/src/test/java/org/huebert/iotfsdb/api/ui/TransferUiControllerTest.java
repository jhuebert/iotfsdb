package org.huebert.iotfsdb.api.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.huebert.iotfsdb.api.ui.service.BasePageService;
import org.huebert.iotfsdb.api.ui.service.ExportUiService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Unit tests for the TransferUiController class.
 */
@ExtendWith(MockitoExtension.class)
class TransferUiControllerTest {

    @Mock
    private ExportUiService exportService;

    @Mock
    private BasePageService basePageService;

    @Mock
    private Model model;

    @InjectMocks
    private TransferUiController controller;

    /**
     * Test the getIndex method returns the correct view name and adds required attributes to the model.
     */
    @Test
    void testGetIndex() {

        // Set up base page data
        BasePageService.BasePage basePage = BasePageService.BasePage.builder()
            .version("2.2.0")
            .build();
        when(basePageService.getBasePage()).thenReturn(basePage);

        // Act
        String viewName = controller.getIndex(model);

        // Assert
        assertEquals("transfer/index", viewName, "Should return the correct view name");
        verify(model).addAttribute(eq("basePage"), eq(basePage));
        verify(basePageService).getBasePage();
    }

    /**
     * Test the exportData method correctly delegates to the ExportUiService.
     */
    @Test
    void testExportData() {
        // Arrange
        ResponseEntity<StreamingResponseBody> expectedResponse = ResponseEntity
            .status(HttpStatus.OK)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"iotfsdb-export.zip\"")
            .body(outputStream -> {
                // Test body that does nothing
            });

        when(exportService.export(null)).thenReturn(expectedResponse);

        // Act
        ResponseEntity<StreamingResponseBody> actualResponse = controller.exportData();

        // Assert
        assertSame(expectedResponse, actualResponse, "Should return the response from ExportUiService");
        verify(exportService).export(null);
    }
}
