package org.huebert.iotfsdb.rest;

import com.github.f4b6a3.ulid.UlidCreator;
import com.google.common.base.Stopwatch;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.ZonedDateTime;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/v1/data")
public class SeriesDataController {

    private final SeriesService seriesService;

    public SeriesDataController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @GetMapping
    public Map<String, Map<ZonedDateTime, ? extends Number>> findData(@Valid DataRequest request) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("findData(request): trace={}, request={}", trace, request);
        Map<String, Map<ZonedDateTime, ? extends Number>> result = seriesService.get(request);
        log.debug("findData(response): trace={}, elapsed={}, size={}", trace, stopwatch.stop(), result.size());
        return result;
    }

}
