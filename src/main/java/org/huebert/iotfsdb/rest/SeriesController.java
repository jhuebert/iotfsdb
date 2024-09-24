package org.huebert.iotfsdb.rest;

import org.huebert.iotfsdb.rest.schema.Series;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("/v1/series")
public class SeriesController {

    @PostMapping
    public Series createSeries(@RequestBody Series series) {
        return series;
    }

    @PutMapping("{id}")
    public Series updateSeries(@PathVariable("id") String id, @RequestBody Series series) {
        return series;
    }

    @DeleteMapping("{id}")
    public void deleteSeries(@PathVariable("id") String id) {
        //TODO Not sure about having this
    }

    @GetMapping("{id}")
    public Series getSeries(@PathVariable("id") String id) {
        return null;
    }

    @GetMapping
    public Series findSeries(String pattern) {
        return null;
    }


}
