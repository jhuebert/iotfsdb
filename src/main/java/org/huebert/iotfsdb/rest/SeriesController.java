package org.huebert.iotfsdb.rest;

import com.github.f4b6a3.ulid.UlidCreator;
import com.google.common.base.Stopwatch;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.springframework.http.HttpStatus.ACCEPTED;
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
    @ResponseStatus(ACCEPTED)
    public void createSeries(@Valid @RequestBody SeriesDefinition definition) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("createSeries(request): trace={}, definition={}", trace, definition);
        seriesService.createSeries(definition);
        log.debug("createSeries(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

    @GetMapping("{id}")
    public SeriesDefinition getSeries(@PathVariable("id") String id) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("getSeries(request): trace={}, id={}", trace, id);
        SeriesDefinition definition = seriesService.getSeriesDefinition(id);
        log.debug("getSeries(response): trace={}, elapsed={}, definition={}", trace, stopwatch.stop(), definition);
        return definition;
    }

    @DeleteMapping("{id}")
    @ResponseStatus(NO_CONTENT)
    public void deleteSeries(@PathVariable("id") String id) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("deleteSeries(request): trace={}, id={}", trace, id);
        seriesService.deleteSeries(id);
        log.debug("deleteSeries(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

    @GetMapping
    public List<SeriesDefinition> findSeries(
        @RequestParam(name = "pattern", required = false, defaultValue = ".*") Pattern pattern,
        @RequestParam(name = "metadata", required = false) Map<String, String> metadata
    ) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("findSeries(request): trace={}, pattern={}, metadata={}", trace, pattern, metadata);
        List<SeriesDefinition> definitions = seriesService.findSeries(pattern, metadata).stream()
            .map(Series::getDefinition)
            .toList();
        log.debug("findSeries(response): trace={}, elapsed={}, definitions={}", trace, stopwatch.stop(), definitions);
        return definitions;
    }

    @GetMapping("{id}/metadata")
    public Map<String, String> getMetadata(@PathVariable("id") String id) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("getMetadata(request): trace={}, id={}", trace, id);
        Map<String, String> metadata = seriesService.getMetadata(id);
        log.debug("getMetadata(response): trace={}, elapsed={}, metadata={}", trace, stopwatch.stop(), metadata);
        return metadata;
    }

    @PutMapping("{id}/metadata")
    @ResponseStatus(ACCEPTED)
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
    @ResponseStatus(ACCEPTED)
    public void set(@PathVariable("id") String id, @NotNull @Valid @RequestBody DataValue dataValue) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("set(request): trace={}, id={}, dataValue={}", trace, id, dataValue);
        seriesService.set(id, dataValue.getDateTime(), dataValue.getValue());
        log.debug("set(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

}
