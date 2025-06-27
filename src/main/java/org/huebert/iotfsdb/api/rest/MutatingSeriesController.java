package org.huebert.iotfsdb.api.rest;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

import static org.huebert.iotfsdb.api.schema.SeriesDefinition.ID_PATTERN;
import static org.springframework.http.HttpStatus.NO_CONTENT;

@Validated
@Slf4j
@RestController
@RequestMapping("/v2/series")
@ConditionalOnProperty(prefix = "iotfsdb", value = "read-only", havingValue = "false")
public class MutatingSeriesController {

    private final SeriesService seriesService;

    public MutatingSeriesController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @CaptureStats(
        group = "rest-v2", type = "series", operation = "create", javaClass = SeriesDataController.class, javaMethod = "createSeries",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/series"),
        }
    )
    @Operation(tags = "Series", summary = "Create new series")
    @PostMapping
    @ResponseStatus(NO_CONTENT)
    public void createSeries(@Valid @RequestBody SeriesFile seriesFile) {
        seriesService.createSeries(seriesFile);
    }

    @CaptureStats(
        group = "rest-v2", type = "series", operation = "delete", javaClass = SeriesDataController.class, javaMethod = "deleteSeries",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "delete"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/series/{id}"),
        }
    )
    @Operation(tags = "Series", summary = "Delete a series")
    @DeleteMapping("{id}")
    @ResponseStatus(NO_CONTENT)
    public void deleteSeries(@PathVariable @Pattern(regexp = ID_PATTERN) String id) {
        seriesService.deleteSeries(id);
    }

    @CaptureStats(
        group = "rest-v2", type = "metadata", operation = "update", javaClass = SeriesDataController.class, javaMethod = "updateMetadata",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "put"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/series/{id}/metadata"),
        }
    )
    @Operation(tags = "Series", summary = "Updates metadata for a series")
    @PutMapping("{id}/metadata")
    @ResponseStatus(NO_CONTENT)
    public void updateMetadata(@PathVariable @Pattern(regexp = ID_PATTERN) String id, @Valid @RequestBody Map<String, String> metadata) {
        seriesService.updateMetadata(id, metadata);
    }

}
