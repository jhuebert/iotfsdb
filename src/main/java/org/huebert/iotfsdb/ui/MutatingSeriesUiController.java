package org.huebert.iotfsdb.ui;

import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.NumberType;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.huebert.iotfsdb.ui.service.BasePageService;
import org.huebert.iotfsdb.ui.service.ObjectEncoder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
@Controller
@RequestMapping("/ui/series")
@ConditionalOnExpression("${iotfsdb.ui:true} and not ${iotfsdb.read-only:false}")
public class MutatingSeriesUiController {

    private final SeriesService seriesService;

    private final ObjectEncoder objectEncoder;

    private final BasePageService basePageService;

    public MutatingSeriesUiController(SeriesService seriesService, ObjectEncoder objectEncoder, BasePageService basePageService) {
        this.seriesService = seriesService;
        this.objectEncoder = objectEncoder;
        this.basePageService = basePageService;
    }

    @CaptureStats(
        id = "ui-series-delete",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "delete"),
            @CaptureStats.Metadata(key = "method", value = "delete"),
        }
    )
    @DeleteMapping("{id}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteSeries(@PathVariable String id) {
        seriesService.deleteSeries(id);
    }

    @CaptureStats(
        id = "ui-series-metadata-delete",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "metadata"),
            @CaptureStats.Metadata(key = "operation", value = "delete"),
            @CaptureStats.Metadata(key = "method", value = "delete"),
        }
    )
    @DeleteMapping("{id}/metadata/{key}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteMetadata(@PathVariable String id, @PathVariable String key) {
        Map<String, String> updatedMetadata = new HashMap<>(getSeries(id).getMetadata());
        updatedMetadata.remove(key);
        seriesService.updateMetadata(id, updatedMetadata);
    }

    @CaptureStats(
        id = "ui-series-metadata-add",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "metadata"),
            @CaptureStats.Metadata(key = "operation", value = "add"),
            @CaptureStats.Metadata(key = "method", value = "post"),
        }
    )
    @PostMapping("{id}/metadata")
    public String addMetadata(Model model, @PathVariable String id, @RequestParam String key, @RequestParam String value) {
        return updateMetadata(model, id, key, value);
    }

    @CaptureStats(
        id = "ui-series-metadata-update",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "metadata"),
            @CaptureStats.Metadata(key = "operation", value = "update"),
            @CaptureStats.Metadata(key = "method", value = "post"),
        }
    )
    @PostMapping("{id}/metadata/{key}")
    public String updateMetadata(Model model, @PathVariable String id, @PathVariable String key, @RequestParam String value) {
        SeriesFile seriesFile = getSeries(id);

        Map<String, String> updatedMetadata = new HashMap<>(seriesFile.getMetadata());
        updatedMetadata.put(key, value);
        seriesService.updateMetadata(id, updatedMetadata);

        model.addAttribute("file", seriesFile);
        model.addAttribute("key", key);
        model.addAttribute("value", value);
        model.addAttribute("basePage", basePageService.getBasePage());

        return "series/fragments/metadata-row";
    }

    @CaptureStats(
        id = "ui-series-create-form",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "create-form"),
            @CaptureStats.Metadata(key = "method", value = "get"),
        }
    )
    @GetMapping("create")
    public String getCreateSeriesForm(Model model) {
        return "series/fragments/create";
    }

    @CaptureStats(
        id = "ui-series-create",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "ui"),
            @CaptureStats.Metadata(key = "type", value = "series"),
            @CaptureStats.Metadata(key = "operation", value = "create"),
            @CaptureStats.Metadata(key = "method", value = "post"),
        }
    )
    @PostMapping
    public String createSeries(
        Model model,
        HttpServletResponse response,
        @RequestParam String id,
        @RequestParam NumberType type,
        @RequestParam Long interval,
        @RequestParam PartitionPeriod partition,
        @RequestParam(required = false) Double min,
        @RequestParam(required = false) Double max
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

        seriesService.createSeries(seriesFile);

        model.addAttribute("series", List.of(seriesFile));

        FindSeriesRequest request = new FindSeriesRequest(Pattern.compile(id), Map.of());
        try {
            response.addHeader("HX-Push-Url", "/ui/series?request=" + objectEncoder.encode(request));
        } catch (IOException e) {
            log.warn("Could not serialize request: {}", request);
        }

        model.addAttribute("basePage", basePageService.getBasePage());

        return "series/fragments/search";
    }

    private SeriesFile getSeries(String seriesId) {
        return seriesService.findSeries(seriesId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Series (" + seriesId + ") does not exist"));
    }

}
