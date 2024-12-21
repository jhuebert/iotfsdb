package org.huebert.iotfsdb.ui;

import com.google.common.net.HttpHeaders;
import org.apache.logging.log4j.util.Strings;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.service.TimeConverter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

@Controller
@RequestMapping("/ui/series")
public class SeriesUiController {

    private final SeriesService seriesService;

    private final ExportService exportService;

    public SeriesUiController(SeriesService seriesService, ExportService exportService) {
        this.seriesService = seriesService;
        this.exportService = exportService;
    }

    @GetMapping
    public String getIndex() {
        return "series/search";
    }

    @PostMapping
    public String search(Model model, @RequestParam("search") String pattern) {
        FindSeriesRequest findSeriesRequest = new FindSeriesRequest();
        if (!Strings.isBlank(pattern)) {
            findSeriesRequest.setPattern(Pattern.compile(".*" + pattern + ".*"));
        }
        model.addAttribute("series", seriesService.findSeries(findSeriesRequest));
        return "series/fragments/results";
    }

    @DeleteMapping("{id}")
    @ResponseStatus(HttpStatus.OK)
    public void delete(@PathVariable("id") String id) {
    }

    @DeleteMapping("{id}/metadata/{key}")
    @ResponseStatus(HttpStatus.OK)
    public void delete(@PathVariable("id") String id, @PathVariable("key") String key) {
    }

    @PostMapping("{id}/metadata")
    public String saveNew(Model model, @PathVariable("id") String id, @RequestParam("key") String key, @RequestParam("value") String value) {
        SeriesFile seriesFile = seriesService.findSeries(id).orElse(null);
        model.addAttribute("file", seriesFile);
        model.addAttribute("key", key);
        model.addAttribute("value", value);
        return "series/fragments/metadata-row";
    }

    @PostMapping("{id}/metadata/{key}")
    public String saveExisting(Model model, @PathVariable("id") String id, @PathVariable("key") String key, @RequestParam("value") String value) {
        SeriesFile seriesFile = seriesService.findSeries(id).orElse(null);
        model.addAttribute("file", seriesFile);
        model.addAttribute("key", key);
        model.addAttribute("value", value);
        return "series/fragments/metadata-row";
    }

    @GetMapping(value = "{id}/export", produces = "application/zip")
    public ResponseEntity<StreamingResponseBody> export(@PathVariable("id") String id) {
        String formattedTime = TimeConverter.toUtc(ZonedDateTime.now()).format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        FindSeriesRequest request = new FindSeriesRequest();
        request.setPattern(Pattern.compile(id));
        return ResponseEntity.ok()
            .header(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=iotfsdb-" + id + "-" + formattedTime + ".zip")
            .contentType(MediaType.parseMediaType("application/zip"))
            .body(out -> {
                try (out) {
                    exportService.export(request, out);
                }
            });
    }

}
