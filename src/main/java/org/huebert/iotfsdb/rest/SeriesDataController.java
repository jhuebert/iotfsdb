package org.huebert.iotfsdb.rest;

import org.huebert.iotfsdb.schema.DataValue;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/data")
public class SeriesDataController {

    private final SeriesService seriesService;

    public SeriesDataController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @PostMapping
    public DataValue set(@RequestBody DataValue dataValue) {
        return seriesService.set(dataValue);
    }
//
//    @GetMapping
//    public List<SeriesData> findData(
//        String pattern,
//        String type,
//        String start,
//        String end,
//        boolean includeNull //TODO Default false?
//    ) {
//
//        return null;
//
//    }


}
