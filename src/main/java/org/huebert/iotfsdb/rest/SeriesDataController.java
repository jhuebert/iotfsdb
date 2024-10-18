package org.huebert.iotfsdb.rest;

import com.github.f4b6a3.ulid.UlidCreator;
import com.google.common.base.Stopwatch;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.rest.schema.FindDataRequest;
import org.huebert.iotfsdb.rest.schema.FindDataResponse;
import org.huebert.iotfsdb.rest.schema.InsertRequest;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.springframework.http.HttpStatus.NO_CONTENT;

@Slf4j
@RestController
@RequestMapping("/v1/data")
public class SeriesDataController {

    private final SeriesService seriesService;

    public SeriesDataController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @GetMapping
    public List<FindDataResponse> find(@Valid FindDataRequest request) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("find(request): trace={}, request={}", trace, request);
        List<FindDataResponse> result = seriesService.find(request);
        log.debug("find(response): trace={}, elapsed={}, size={}", trace, stopwatch.stop(), result.size());
        return result;
    }

    @PostMapping
    @ResponseStatus(NO_CONTENT)
    public void insert(@NotNull List<InsertRequest> request) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String trace = UlidCreator.getUlid().toLowerCase();
        log.debug("insert(request): trace={}, request={}", trace, request.size());
        request.parallelStream()
            .forEach(e -> seriesService.insert(e.getSeries(), e.getValues()));
        log.debug("insert(response): trace={}, elapsed={}", trace, stopwatch.stop());
    }

}
