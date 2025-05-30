package org.huebert.iotfsdb.ui;

import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.huebert.iotfsdb.ui.service.BasePageService;
import org.huebert.iotfsdb.ui.service.ExportUiService;
import org.huebert.iotfsdb.ui.service.ObjectEncoder;
import org.huebert.iotfsdb.ui.service.SearchParser;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.util.List;

@Slf4j
@Controller
@RequestMapping("/ui/series")
@ConditionalOnProperty(prefix = "iotfsdb", value = "ui", havingValue = "true")
public class SeriesUiController {

    private final SeriesService seriesService;

    private final ExportUiService exportService;

    private final ObjectEncoder objectEncoder;

    private final BasePageService basePageService;

    public SeriesUiController(SeriesService seriesService, ExportUiService exportService, ObjectEncoder objectEncoder, BasePageService basePageService) {
        this.seriesService = seriesService;
        this.exportService = exportService;
        this.objectEncoder = objectEncoder;
        this.basePageService = basePageService;
    }

    @CaptureStats(
        id = "ui-series-index",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "index"),
            @CaptureStats.Metadata(key = "method", value = "get"),
        }
    )
    @GetMapping
    public String getIndex(Model model, @RequestParam(value = "request", required = false) String request) {
        List<SeriesFile> series = List.of();
        try {
            if (request != null) {
                FindSeriesRequest findSeriesRequest = objectEncoder.decode(request, FindSeriesRequest.class);
                series = seriesService.findSeries(findSeriesRequest);
                model.addAttribute("request", findSeriesRequest);
            }
        } catch (IOException e) {
            log.warn("Could not parse request: {}", request);
        }
        model.addAttribute("series", series);
        model.addAttribute("basePage", basePageService.getBasePage());
        return "series/index";
    }

    @CaptureStats(
        id = "ui-series-search",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "search"),
            @CaptureStats.Metadata(key = "method", value = "post"),
        }
    )
    @PostMapping("search")
    public String search(Model model, HttpServletResponse response, @RequestParam("search") String search) {
        FindSeriesRequest request = SearchParser.fromSearch(search);
        model.addAttribute("series", seriesService.findSeries(request));
        try {
            response.addHeader("HX-Push-Url", "/ui/series?request=" + objectEncoder.encode(request));
        } catch (IOException e) {
            log.warn("Could not serialize request: {}", request);
        }
        model.addAttribute("basePage", basePageService.getBasePage());
        return "series/fragments/results";
    }

    @CaptureStats(
        id = "ui-series-export",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "export"),
            @CaptureStats.Metadata(key = "method", value = "get"),
        }
    )
    @GetMapping("{id}/export")
    public ResponseEntity<StreamingResponseBody> exportSeries(@PathVariable String id) {
        return exportService.export(id);
    }

}
