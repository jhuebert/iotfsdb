package org.huebert.iotfsdb.ui;

import com.google.common.net.HttpHeaders;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.TimeConverter;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

@Service
public class ExportUiService {

    private final ExportService exportService;

    public ExportUiService(ExportService exportService) {
        this.exportService = exportService;
    }

    public ResponseEntity<StreamingResponseBody> export(String id) {
        String formattedTime = TimeConverter.toUtc(ZonedDateTime.now()).format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        FindSeriesRequest request = new FindSeriesRequest();
        String filename = "iotfsdb-" + formattedTime + ".zip";
        if (id != null) {
            request.setPattern(Pattern.compile(id));
            filename = "iotfsdb-" + id + "-" + formattedTime + ".zip";
        }
        return ResponseEntity.ok()
            .header(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + filename)
            .contentType(MediaType.parseMediaType("application/zip"))
            .body(out -> {
                try (out) {
                    exportService.export(request, out);
                }
            });
    }
}
