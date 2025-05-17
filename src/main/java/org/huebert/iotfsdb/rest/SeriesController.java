package org.huebert.iotfsdb.rest;

import static org.huebert.iotfsdb.schema.SeriesDefinition.ID_PATTERN;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.SeriesService;
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

@Validated
@Slf4j
@RestController
@RequestMapping("/v2/series")
public class SeriesController {

    private final SeriesService seriesService;

    public SeriesController(@NotNull SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @Operation(tags = "Series", summary = "Finds series that match search parameters")
    @PostMapping("find")
    public List<SeriesFile> find(@Valid @RequestBody FindSeriesRequest request) {
        return seriesService.findSeries(request);
    }

    @Operation(tags = "Series", summary = "Get series details")
    @GetMapping("{id}")
    public SeriesFile get(@PathVariable @Pattern(regexp = ID_PATTERN) String id) {
        return getSeries(id);
    }

    @Operation(tags = "Series", summary = "Retrieves the metadata for a series")
    @GetMapping("{id}/metadata")
    public Map<String, String> getMetadata(@PathVariable @Pattern(regexp = ID_PATTERN) String id) {
        return getSeries(id).getMetadata();
    }

    private SeriesFile getSeries(String seriesId) {
        return seriesService.findSeries(seriesId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "series (%s) does not exist".formatted(seriesId)));
    }

}
