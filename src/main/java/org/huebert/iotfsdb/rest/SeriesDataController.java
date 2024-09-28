package org.huebert.iotfsdb.rest;

import org.huebert.iotfsdb.rest.schema.SeriesData;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController("/v1/data")
public class SeriesDataController {

    @PostMapping
    public IntegerValue addData(IntegerValue seriesData) {
        return null;
    }

    @GetMapping
    public List<SeriesData> findData(
        String pattern,
        String type,
        String start,
        String end,
        boolean includeNull //TODO Default false?
    ) {

        return null;

    }



}
