package org.huebert.iotfsdb.rest;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.InsertRequest;
import org.huebert.iotfsdb.service.InsertService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.springframework.http.HttpStatus.NO_CONTENT;

@Validated
@Slf4j
@RestController
@RequestMapping("/v2/data")
@ConditionalOnProperty(prefix = "iotfsdb", value = "read-only", havingValue = "false")
public class MutatingSeriesDataController {

    private final InsertService insertService;

    public MutatingSeriesDataController(@NotNull InsertService insertService) {
        this.insertService = insertService;
    }

    @Operation(tags = "Data", summary = "Bulk insert of data")
    @PostMapping("batch")
    @ResponseStatus(NO_CONTENT)
    public void insert(@NotNull @Valid @RequestBody List<InsertRequest> request) {
        request.parallelStream() // TODO Test the impact of parallel
            .forEach(e -> insertService.insert(e.getSeries(), e.getValues()));
    }

}
