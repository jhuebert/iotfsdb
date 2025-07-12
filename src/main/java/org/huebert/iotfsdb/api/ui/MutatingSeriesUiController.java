package org.huebert.iotfsdb.api.ui;

import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.api.ui.service.BasePageService;
import org.huebert.iotfsdb.api.ui.service.ObjectEncoder;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
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
@ConditionalOnExpression("${iotfsdb.api.ui:true} and not ${iotfsdb.read-only:false}")
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
        group = "ui", type = "series", operation = "delete", javaClass = MutatingSeriesUiController.class, javaMethod = "deleteSeries",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "delete"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui/series/{id}"),
        }
    )
    @DeleteMapping("{id}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteSeries(@PathVariable String id) {
        seriesService.deleteSeries(id);
    }

    @CaptureStats(
        group = "ui", type = "metadata", operation = "delete", javaClass = MutatingSeriesUiController.class, javaMethod = "deleteMetadata",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "delete"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui/series/{id}/metadata/{key}"),
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
        group = "ui", type = "metadata", operation = "add", javaClass = MutatingSeriesUiController.class, javaMethod = "addMetadata",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui/series/{id}/metadata"),
        }
    )
    @PostMapping("{id}/metadata")
    public String addMetadata(Model model, @PathVariable String id, @RequestParam String key, @RequestParam String value) {
        return updateMetadata(model, id, key, value);
    }

    @CaptureStats(
        group = "ui", type = "metadata", operation = "update", javaClass = MutatingSeriesUiController.class, javaMethod = "updateMetadata",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui/series/{id}/metadata/{key}"),
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
        group = "ui", type = "series", operation = "form", javaClass = MutatingSeriesUiController.class, javaMethod = "getCreateSeriesForm",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "get"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui/series/create"),
        }
    )
    @GetMapping("create")
    public String getCreateSeriesForm(Model model) {
        return "series/fragments/create";
    }

    @CaptureStats(
        group = "ui", type = "series", operation = "create", javaClass = MutatingSeriesUiController.class, javaMethod = "createSeries",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
            @CaptureStats.Metadata(key = "restPath", value = "/ui/series"),
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
