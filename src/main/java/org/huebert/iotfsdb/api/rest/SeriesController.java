package org.huebert.iotfsdb.api.rest;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

import static org.huebert.iotfsdb.api.schema.SeriesDefinition.ID_PATTERN;

@Validated
@Slf4j
@RestController
@RequestMapping("/v2/series")
public class SeriesController {

    private final SeriesService seriesService;

    public SeriesController(@NotNull SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @CaptureStats(
        group = "rest-v2", type = "series", operation = "find", javaClass = SeriesController.class, javaMethod = "findSeries",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/series/find"),
        }
    )
    @Operation(tags = "Series", summary = "Finds series that match search parameters")
    @PostMapping("find")
    public List<SeriesFile> findSeries(@Valid @RequestBody FindSeriesRequest request) {
        return seriesService.findSeries(request);
    }

    @CaptureStats(
        group = "rest-v2", type = "series", operation = "get", javaClass = SeriesController.class, javaMethod = "getSeries",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "get"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/series/{id}"),
        }
    )
    @Operation(tags = "Series", summary = "Get series details")
    @GetMapping("{id}")
    public SeriesFile getSeries(@PathVariable @Pattern(regexp = ID_PATTERN) String id) {
        return getSeriesFile(id);
    }

    @CaptureStats(
        group = "rest-v2", type = "metadata", operation = "get", javaClass = SeriesController.class, javaMethod = "getMetadata",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "get"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/series/{id}/metadata"),
        }
    )
    @Operation(tags = "Series", summary = "Retrieves the metadata for a series")
    @GetMapping("{id}/metadata")
    public Map<String, String> getMetadata(@PathVariable @Pattern(regexp = ID_PATTERN) String id) {
        return getSeriesFile(id).getMetadata();
    }

    private SeriesFile getSeriesFile(String seriesId) {
        return seriesService.findSeries(seriesId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "series (%s) does not exist".formatted(seriesId)));
    }

}
