package org.huebert.iotfsdb.ui;

import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.NumberType;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
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

    @DeleteMapping("{id}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteSeries(@PathVariable("id") String id) {
        seriesService.deleteSeries(id);
    }

    @DeleteMapping("{id}/metadata/{key}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteMetadata(@PathVariable("id") String id, @PathVariable("key") String key) {
        SeriesFile seriesFile = getSeries(id);
        Map<String, String> updatedMetadata = new HashMap<>(seriesFile.getMetadata());
        updatedMetadata.remove(key);
        seriesService.updateMetadata(id, updatedMetadata);
    }

    @PostMapping("{id}/metadata")
    public String addMetadata(Model model, @PathVariable("id") String id, @RequestParam("key") String key, @RequestParam("value") String value) {
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

    @PostMapping("{id}/metadata/{key}")
    public String updateMetadata(Model model, @PathVariable("id") String id, @PathVariable("key") String key, @RequestParam("value") String value) {
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

    @GetMapping("create")
    public String getCreateSeriesForm(Model model) {
        return "series/fragments/create";
    }

    @PostMapping
    public String createSeries(
        Model model,
        HttpServletResponse response,
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

        seriesService.createSeries(seriesFile);

        model.addAttribute("series", List.of(seriesFile));

        FindSeriesRequest request = new FindSeriesRequest(Pattern.compile(id), Map.of());
        try {
            response.addHeader("HX-Push-Url", "/ui/series?request=" + objectEncoder.encode(request));
        } catch (IOException e) {
            log.warn("could not serialize {}", request);
        }

        model.addAttribute("basePage", basePageService.getBasePage());

        return "series/fragments/search";
    }

    private SeriesFile getSeries(String seriesId) {
        return seriesService.findSeries(seriesId).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, String.format("series (%s) does not exist", seriesId)));
    }

}
