package org.huebert.iotfsdb.rest;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.rest.schema.ArchiveRequest;
import org.huebert.iotfsdb.rest.schema.FindSeriesRequest;
import org.huebert.iotfsdb.rest.schema.FindSeriesResponse;
import org.huebert.iotfsdb.rest.schema.ReduceRequest;
import org.huebert.iotfsdb.rest.schema.SeriesData;
import org.huebert.iotfsdb.rest.schema.SeriesStats;
import org.huebert.iotfsdb.series.Series;
import org.huebert.iotfsdb.series.SeriesDefinition;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NO_CONTENT;

@Slf4j
@RestController
@RequestMapping("/v1/series")
public class SeriesController {

    private final SeriesService seriesService;

    public SeriesController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @Operation(summary = "Create new series", description = "")
    @PostMapping
    @ResponseStatus(NO_CONTENT)
    public void create(@NotNull @Valid @RequestBody SeriesDefinition definition) {
        log.debug("create(enter): definition={}", definition);
        seriesService.createSeries(definition);
        log.debug("create(exit)");
    }

    @Operation(summary = "Get series details", description = "")
    @GetMapping("{id}")
    public FindSeriesResponse get(@PathVariable("id") String id) {
        log.debug("get(enter): id={}", id);
        Series series = seriesService.getSeries(id);
        FindSeriesResponse result = FindSeriesResponse.builder()
            .definition(series.getDefinition())
            .metadata(series.getMetadata())
            .build();
        log.debug("get(exit): result={}", result);
        return result;
    }

    @DeleteMapping("{id}")
    @ResponseStatus(NO_CONTENT)
    public void delete(@PathVariable("id") String id) {
        log.debug("delete(enter): id={}", id);
        seriesService.deleteSeries(id);
        log.debug("delete(exit)");
    }

    @GetMapping
    public List<FindSeriesResponse> find(@NotNull @Valid FindSeriesRequest request) {
        log.debug("find(enter): request={}", request);
        List<FindSeriesResponse> result = seriesService.findSeries(request.getPattern(), request.getMetadata()).stream()
            .map(s -> FindSeriesResponse.builder().definition(s.getDefinition()).metadata(s.getMetadata()).build())
            .toList();
        log.debug("find(exit): result={}", result.size());
        return result;
    }

    @GetMapping("{id}/metadata")
    public Map<String, String> getMetadata(@PathVariable("id") String id) {
        log.debug("getMetadata(enter): id={}", id);
        Map<String, String> metadata = seriesService.getSeries(id).getMetadata();
        log.debug("getMetadata(exit): metadata={}", metadata);
        return metadata;
    }

    @PutMapping("{id}/metadata")
    @ResponseStatus(NO_CONTENT)
    public void updateMetadata(@PathVariable("id") String id, @NotNull @Valid @RequestBody Map<String, String> metadata) {
        log.debug("updateMetadata(enter): id={}, metadata={}", id, metadata);
        if (metadata == null) {
            throw new ResponseStatusException(BAD_REQUEST, "metadata is null");
        }
        seriesService.updateMetadata(id, metadata);
        log.debug("updateMetadata(exit)");
    }

    @PostMapping("{id}/reduce")
    @ResponseStatus(NO_CONTENT)
    public void reduce(@PathVariable("id") String id, @NotNull @Valid @RequestBody ReduceRequest request) {
        log.debug("reduce(enter): id={}, request={}", id, request);
        // TODO
        log.debug("reduce(exit)");
    }

    @GetMapping("{id}/stats")
    public SeriesStats stats(@PathVariable("id") String id) {
        log.debug("stats(enter): id={}", id);
        SeriesStats result = seriesService.getSeriesStats(id);
        log.debug("stats(exit): result={}", result);
        return result;
    }

    @PostMapping("{id}/unarchive")
    @ResponseStatus(NO_CONTENT)
    public void unarchive(@PathVariable("id") String id, @NotNull @Valid @RequestBody ArchiveRequest request) {
        log.debug("unarchive(enter): id={}, request={}", id, request);
        seriesService.unarchiveSeries(id, request);
        log.debug("unarchive(exit)");
    }

    @PostMapping("{id}/archive")
    @ResponseStatus(NO_CONTENT)
    public void archive(@PathVariable("id") String id, @NotNull @Valid @RequestBody ArchiveRequest request) {
        log.debug("archive(enter): id={}, request={}", id, request);
        seriesService.archiveSeries(id, request);
        log.debug("archive(exit)");
    }

    @PostMapping("{id}/data")
    @ResponseStatus(NO_CONTENT)
    public void insert(@PathVariable("id") String id, @NotNull @Valid @RequestBody SeriesData dataValue) {
        log.debug("insert(enter): id={}, dataValue={}", id, dataValue);
        seriesService.insert(id, dataValue);
        log.debug("insert(exit)");
    }

    @PostMapping("{id}/data/batch")
    @ResponseStatus(NO_CONTENT)
    public void insert(@PathVariable("id") String id, @NotNull @Valid @RequestBody List<SeriesData> dataValues) {
        log.debug("insert(enter): id={}, dataValues={}", id, dataValues.size());
        seriesService.insert(id, dataValues);
        log.debug("insert(exit)");
    }

}
