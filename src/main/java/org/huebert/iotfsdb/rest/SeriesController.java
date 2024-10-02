package org.huebert.iotfsdb.rest;

import org.huebert.iotfsdb.schema.DataValue;
import org.huebert.iotfsdb.schema.Series;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NO_CONTENT;

@RestController
@RequestMapping("/v1/series")
public class SeriesController {

    private final SeriesService seriesService;

    public SeriesController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @PostMapping
    public Series createSeries(@RequestBody Series series) throws IOException {
        Series.checkValid(series);
        return seriesService.createSeries(series);
    }

    @GetMapping("{id}")
    public Series getSeries(@PathVariable("id") String id) {
        return seriesService.getSeries(id);
    }

    @DeleteMapping("{id}")
    @ResponseStatus(NO_CONTENT)
    public void deleteSeries(@PathVariable("id") String id) {
        seriesService.deleteSeries(id);
    }

    @GetMapping
    public List<Series> findSeries(
        @RequestParam(name = "pattern", required = false, defaultValue = ".*") Pattern pattern,
        @RequestParam Map<String, String> metadata
    ) {
        Map<String, String> trimmedMetadata = new HashMap<>(metadata);
        trimmedMetadata.keySet().remove("pattern");
        return seriesService.findSeries(pattern, trimmedMetadata);
    }

    @GetMapping("{id}/metadata")
    public Map<String, String> getMetadata(@PathVariable("id") String id) {
        return seriesService.getMetadata(id);
    }

    @PutMapping("{id}/metadata")
    public Map<String, String> updateMetadata(@PathVariable("id") String id, @RequestBody Map<String, String> metadata) throws IOException {
        if (metadata == null) {
            throw new ResponseStatusException(BAD_REQUEST, "metadata is null");
        }
        return seriesService.updateMetadata(id, metadata);
    }

    @PostMapping("{id}/data")
    public DataValue set(@PathVariable("id") String id, @RequestBody DataValue dataValue) {
        DataValue.checkValid(dataValue);
        seriesService.set(id, dataValue.dateTime(), dataValue.value());
        return dataValue;
    }

}
