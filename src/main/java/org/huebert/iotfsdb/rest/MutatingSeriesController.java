package org.huebert.iotfsdb.rest;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
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

import static org.huebert.iotfsdb.schema.SeriesDefinition.ID_PATTERN;
import static org.springframework.http.HttpStatus.NO_CONTENT;

@Validated
@Slf4j
@RestController
@RequestMapping("/v2/series")
@ConditionalOnProperty(prefix = "iotfsdb", value = "read-only", havingValue = "false")
public class MutatingSeriesController {

    private final SeriesService seriesService;

    public MutatingSeriesController(@NotNull SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @Operation(tags = "Series", summary = "Create new series")
    @PostMapping
    @ResponseStatus(NO_CONTENT)
    public void create(@NotNull @Valid @RequestBody SeriesFile seriesFile) {
        seriesService.createSeries(seriesFile);
    }

    @Operation(tags = "Series", summary = "Delete a series")
    @DeleteMapping("{id}")
    @ResponseStatus(NO_CONTENT)
    public void delete(@PathVariable("id") @Pattern(regexp = ID_PATTERN) String id) {
        seriesService.deleteSeries(id);
    }

    @Operation(tags = "Series", summary = "Updates metadata for a series")
    @PutMapping("{id}/metadata")
    @ResponseStatus(NO_CONTENT)
    public void updateMetadata(@PathVariable("id") @Pattern(regexp = ID_PATTERN) String id, @NotNull @Valid @RequestBody Map<String, String> metadata) {
        seriesService.updateMetadata(id, metadata);
    }

}
