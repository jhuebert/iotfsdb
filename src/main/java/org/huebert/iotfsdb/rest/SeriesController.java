package org.huebert.iotfsdb.rest;

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

import java.util.HashMap;
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
    public SeriesDefinition createSeries(@RequestBody SeriesDefinition seriesDefinition) {
        log.info("createSeries: seriesDefinition={}", seriesDefinition);
        SeriesDefinition.checkValid(seriesDefinition);
        return seriesService.createSeries(seriesDefinition);
    }

    @GetMapping("{id}")
    public SeriesDefinition getSeries(@PathVariable("id") String id) {
        log.info("getSeries: id={}", id);
        return seriesService.getSeriesDefinition(id);
    }

    @DeleteMapping("{id}")
    @ResponseStatus(NO_CONTENT)
    public void deleteSeries(@PathVariable("id") String id) {
        log.info("deleteSeries: id={}", id);
        seriesService.deleteSeries(id);
    }

    @GetMapping
    public List<SeriesDefinition> findSeries(
        @RequestParam(name = "pattern", required = false, defaultValue = ".*") Pattern pattern,
        @RequestParam Map<String, String> metadata
    ) {
        Map<String, String> trimmedMetadata = new HashMap<>(metadata);
        trimmedMetadata.keySet().remove("pattern");
        log.info("findSeries: pattern={}, metadata={}", pattern, trimmedMetadata);
        return seriesService.findSeries(pattern, trimmedMetadata).stream()
            .map(Series::getDefinition)
            .toList();
    }

    @GetMapping("{id}/metadata")
    public Map<String, String> getMetadata(@PathVariable("id") String id) {
        log.info("getMetadata: id={}", id);
        return seriesService.getMetadata(id);
    }

    @PutMapping("{id}/metadata")
    @ResponseStatus(ACCEPTED)
    public void updateMetadata(@PathVariable("id") String id, @RequestBody Map<String, String> metadata) {
        log.info("updateMetadata: id={}, metadata={}", id, metadata);
        if (metadata == null) {
            throw new ResponseStatusException(BAD_REQUEST, "metadata is null");
        }
        seriesService.updateMetadata(id, metadata);
    }

    @PostMapping("{id}/data")
    @ResponseStatus(ACCEPTED)
    public void set(@PathVariable("id") String id, @RequestBody DataValue dataValue) {
        log.info("set: id={}, dataValue={}", id, dataValue);
        DataValue.checkValid(dataValue);
        seriesService.set(id, dataValue.dateTime(), dataValue.value());
    }

}
