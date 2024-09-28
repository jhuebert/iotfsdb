package org.huebert.iotfsdb.rest;

import org.huebert.iotfsdb.rest.schema.Series;
import org.huebert.iotfsdb.rest.schema.SeriesMetadata;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController("/v1/series")
public class SeriesController {

    private final SeriesService seriesService;

    public SeriesController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @PostMapping
    public Series createSeries(@RequestBody Series series) {
        return seriesService.createSeries(series);
    }

    @GetMapping("{id}")
    public Series getSeries(@PathVariable("id") String id) {
        return seriesService.getSeries(id);
    }

    @DeleteMapping("{id}")
    public void deleteSeries(@PathVariable("id") String id) {
        seriesService.deleteSeries(id);
    }

    @GetMapping
    public List<Series> findSeries(String pattern) {
        return seriesService.findSeries(pattern);
    }

    @GetMapping("{id}/metadata")
    public SeriesMetadata getMetadata(@PathVariable("id") String id) {
        return null;
    }

    @PutMapping("{id}/metadata")
    public SeriesMetadata updateMetadata(@PathVariable("id") String id, @RequestBody SeriesMetadata metadata) {
        return metadata;
    }

}
