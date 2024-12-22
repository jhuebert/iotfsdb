package org.huebert.iotfsdb.ui;

import org.apache.logging.log4j.util.Strings;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.NumberType;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.http.HttpStatus;
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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Controller
@RequestMapping("/ui/series")
public class SeriesUiController {

    private final SeriesService seriesService;

    private final ExportUiService exportService;

    public SeriesUiController(SeriesService seriesService, ExportUiService exportService) {
        this.seriesService = seriesService;
        this.exportService = exportService;
    }

    @GetMapping
    public String getIndex(Model model) {
        model.addAttribute("series", List.of());
        return "series/index";
    }

    @PostMapping("search")
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
        return exportService.export(id);
    }

    @PostMapping
    public String createSeries(
        Model model,
        @RequestParam("id") String id,
        @RequestParam("type") NumberType type,
        @RequestParam("interval") Long interval,
        @RequestParam("partition") PartitionPeriod partition,
        @RequestParam(value = "min", required = false) Double min,
        @RequestParam(value = "max", required = false) Double max
    ) {
        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id(id)
                .type(type)
                .interval(interval)
                .partition(partition)
                .min(min)
                .max(max)
                .build())
            .metadata(Map.of())
            .build();
        model.addAttribute("series", List.of(seriesFile));
        return "series/fragments/search";
    }

    @GetMapping("create")
    public String createSeriesForm(Model model) {
        return "series/fragments/create";
    }

}
