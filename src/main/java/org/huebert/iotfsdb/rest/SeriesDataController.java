package org.huebert.iotfsdb.rest;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.rest.schema.FindDataRequest;
import org.huebert.iotfsdb.rest.schema.FindDataResponse;
import org.huebert.iotfsdb.rest.schema.InsertRequest;
import org.huebert.iotfsdb.rest.schema.SeriesStats;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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

    @Operation(tags = "Data", summary = "Finds data matching the input request")
    @GetMapping
    public List<FindDataResponse> find(@NotNull @Valid FindDataRequest request) {
        log.debug("find(enter): request={}", request);
        List<FindDataResponse> result = seriesService.find(request);
        log.debug("find(exit): size={}", result.size());
        return result;
    }

    @Operation(tags = "Data", summary = "Bulk insert of data")
    @PostMapping
    @ResponseStatus(NO_CONTENT)
    public void insert(@NotNull @Valid @RequestBody List<InsertRequest> request) {
        log.debug("insert(enter): request={}", request.size());
        request.parallelStream()
            .forEach(e -> seriesService.insert(e.getSeries(), e.getValues()));
        log.debug("insert(exit)");
    }

    @Operation(tags = "Data", summary = "Retrieves series statistics for the entire database")
    @GetMapping("stats")
    public SeriesStats stats() {
        log.debug("stats(enter)");
        SeriesStats result = seriesService.getCombinedStats();
        log.debug("stats(exit): result={}", result);
        return result;
    }

}
