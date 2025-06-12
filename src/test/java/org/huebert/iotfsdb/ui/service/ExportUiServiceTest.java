package org.huebert.iotfsdb.ui.service;

import com.google.common.net.HttpHeaders;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.service.ExportService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ExportUiServiceTest {

    @Mock
    private ExportService exportService;

    @InjectMocks
    private ExportUiService exportUiService;

    @Test
    public void testExportWithId() throws Exception {

        doAnswer(a -> {
            OutputStream out = a.getArgument(1);
            PrintStream printStream = new PrintStream(out);
            printStream.print("test-output");
            return null;
        }).when(exportService).export(any(), any());

        ResponseEntity<StreamingResponseBody> export = exportUiService.export("test-series");

        assertThat(export.getHeaders().get(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS)).isEqualTo(List.of(HttpHeaders.CONTENT_DISPOSITION));
        assertThat(export.getHeaders().get(HttpHeaders.CONTENT_TYPE)).isEqualTo(List.of("application/octet-stream"));
        assertThat(export.getHeaders().get(HttpHeaders.CONTENT_DISPOSITION)).matches(a -> (a.size() == 1) && a.getFirst().matches("attachment;filename=iotfsdb-test-series-.*-.*\\.zip"));

        ByteArrayOutputStream testOutputStream = new ByteArrayOutputStream();
        export.getBody().writeTo(testOutputStream);
        assertThat(testOutputStream.toString()).isEqualTo("test-output");

        ArgumentCaptor<FindSeriesRequest> requestCaptor = ArgumentCaptor.forClass(FindSeriesRequest.class);
        verify(exportService).export(requestCaptor.capture(), any());
        assertThat(requestCaptor.getValue().getPattern().toString()).isEqualTo("test-series");
    }

    @Test
    public void testExportAll() throws Exception {

        doAnswer(a -> {
            OutputStream out = a.getArgument(1);
            PrintStream printStream = new PrintStream(out);
            printStream.print("test-output");
            return null;
        }).when(exportService).export(any(), any());

        ResponseEntity<StreamingResponseBody> export = exportUiService.export(null);

        assertThat(export.getHeaders().get(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS)).isEqualTo(List.of(HttpHeaders.CONTENT_DISPOSITION));
        assertThat(export.getHeaders().get(HttpHeaders.CONTENT_TYPE)).isEqualTo(List.of("application/octet-stream"));
        assertThat(export.getHeaders().get(HttpHeaders.CONTENT_DISPOSITION)).matches(a -> (a.size() == 1) && a.getFirst().matches("attachment;filename=iotfsdb-.*-.*\\.zip"));

        ByteArrayOutputStream testOutputStream = new ByteArrayOutputStream();
        export.getBody().writeTo(testOutputStream);
        assertThat(testOutputStream.toString()).isEqualTo("test-output");

        ArgumentCaptor<FindSeriesRequest> requestCaptor = ArgumentCaptor.forClass(FindSeriesRequest.class);
        verify(exportService).export(requestCaptor.capture(), any());
        assertThat(requestCaptor.getValue().getPattern().toString()).isEqualTo(".*");
    }

}
