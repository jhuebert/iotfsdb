package org.huebert.iotfsdb.rest;

import com.github.f4b6a3.ulid.UlidCreator;
import com.google.common.base.Stopwatch;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.rest.schema.FindSeriesRequest;
import org.huebert.iotfsdb.rest.schema.FindSeriesResponse;
import org.huebert.iotfsdb.rest.schema.SeriesData;
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

    @PostMapping
    @ResponseStatus(NO_CONTENT)
    public void create(@Valid @RequestBody SeriesDefinition definition) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("create(request): trace={}, definition={}", trace, definition);
        seriesService.createSeries(definition);
        log.debug("create(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

    @GetMapping("{id}")
    public FindSeriesResponse get(@PathVariable("id") String id) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("get(request): trace={}, id={}", trace, id);
        Series series = seriesService.getSeries(id);
        FindSeriesResponse result = FindSeriesResponse.builder()
            .definition(series.getDefinition())
            .metadata(series.getMetadata())
            .build();
        log.debug("get(response): trace={}, elapsed={}, result={}", trace, stopwatch.stop(), result);
        return result;
    }

    @DeleteMapping("{id}")
    @ResponseStatus(NO_CONTENT)
    public void delete(@PathVariable("id") String id) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("delete(request): trace={}, id={}", trace, id);
        seriesService.deleteSeries(id);
        log.debug("delete(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

    @GetMapping
    public List<FindSeriesResponse> find(@Valid FindSeriesRequest request) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("find(request): trace={}, request={}", trace, request);
        List<FindSeriesResponse> result = seriesService.findSeries(request.getPattern(), request.getMetadata()).stream()
            .map(s -> FindSeriesResponse.builder().definition(s.getDefinition()).metadata(s.getMetadata()).build())
            .toList();
        log.debug("find(response): trace={}, elapsed={}, result={}", trace, stopwatch.stop(), result.size());
        return result;
    }

    @GetMapping("{id}/metadata")
    public Map<String, String> getMetadata(@PathVariable("id") String id) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("getMetadata(request): trace={}, id={}", trace, id);
        Map<String, String> metadata = seriesService.getSeries(id).getMetadata();
        log.debug("getMetadata(response): trace={}, elapsed={}, metadata={}", trace, stopwatch.stop(), metadata);
        return metadata;
    }

    @PutMapping("{id}/metadata")
    @ResponseStatus(NO_CONTENT)
    public void updateMetadata(@PathVariable("id") String id, @NotNull @RequestBody Map<String, String> metadata) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("updateMetadata(request): trace={}, id={}, metadata={}", trace, id, metadata);
        if (metadata == null) {
            throw new ResponseStatusException(BAD_REQUEST, "metadata is null");
        }
        seriesService.updateMetadata(id, metadata);
        log.debug("updateMetadata(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

    @PostMapping("{id}/data")
    @ResponseStatus(NO_CONTENT)
    public void insert(@PathVariable("id") String id, @NotNull @Valid @RequestBody SeriesData dataValue) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("insert(request): trace={}, id={}, dataValue={}", trace, id, dataValue);
        seriesService.insert(id, dataValue);
        log.debug("insert(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

    @PostMapping("{id}/data/batch")
    @ResponseStatus(NO_CONTENT)
    public void insert(@PathVariable("id") String id, @NotNull @Valid @RequestBody List<SeriesData> dataValues) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("insert(request): trace={}, id={}, dataValues={}", trace, id, dataValues.size());
        seriesService.insert(id, dataValues);
        log.debug("insert(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

}
