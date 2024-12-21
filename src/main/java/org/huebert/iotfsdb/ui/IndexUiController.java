package org.huebert.iotfsdb.ui;

import com.google.common.net.HttpHeaders;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.TimeConverter;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.servlet.view.RedirectView;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

//TODO Conditionally load UI
@Controller
@RequestMapping("/ui")
public class IndexUiController {

    private final ExportService exportService;

    public IndexUiController(ExportService exportService) {
        this.exportService = exportService;
    }

    @GetMapping
    public RedirectView getIndex(Model model) {
        return new RedirectView("/ui/series");
    }

    @GetMapping(value = "export", produces = "application/zip")
    public ResponseEntity<StreamingResponseBody> export() {
        String formattedTime = TimeConverter.toUtc(ZonedDateTime.now()).format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        FindSeriesRequest request = new FindSeriesRequest();
        return ResponseEntity.ok()
            .header(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=iotfsdb-" + formattedTime + ".zip")
            .contentType(MediaType.parseMediaType("application/zip"))
            .body(out -> {
                try (out) {
                    exportService.export(request, out);
                }
            });
    }

}
