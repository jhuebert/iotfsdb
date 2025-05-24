package org.huebert.iotfsdb.rest;

import static org.springframework.http.HttpStatus.NO_CONTENT;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.InsertRequest;
import org.huebert.iotfsdb.service.ImportService;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.ParallelUtil;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Validated
@Slf4j
@RestController
@RequestMapping("/v2/data")
@ConditionalOnProperty(prefix = "iotfsdb", value = "read-only", havingValue = "false")
public class MutatingSeriesDataController {

    private final InsertService insertService;

    private final ImportService importService;

    public MutatingSeriesDataController(InsertService insertService, ImportService importService) {
        this.insertService = insertService;
        this.importService = importService;
    }

    @CaptureStats(
        id = "api-data-insert",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "api"),
            @CaptureStats.Metadata(key = "type", value = "data"),
            @CaptureStats.Metadata(key = "operation", value = "insert"),
            @CaptureStats.Metadata(key = "method", value = "post"),
        }
    )
    @Operation(tags = "Data", summary = "Bulk insert of data")
    @PostMapping
    @ResponseStatus(NO_CONTENT)
    public void insertData(@Valid @RequestBody List<InsertRequest> request) {
        ParallelUtil.forEach(request, insertService::insert);
    }

    @CaptureStats(
        id = "api-data-import",
        metadata = {
            @CaptureStats.Metadata(key = "group", value = "api"),
            @CaptureStats.Metadata(key = "type", value = "data"),
            @CaptureStats.Metadata(key = "operation", value = "import"),
            @CaptureStats.Metadata(key = "method", value = "post"),
        }
    )
    @Operation(tags = "Data", summary = "Imports a database archive")
    @PostMapping("import")
    @ResponseStatus(NO_CONTENT)
    public void importData(@RequestParam("file") MultipartFile file) throws IOException {

        if ((file.getOriginalFilename() != null) && !file.getOriginalFilename().endsWith(".zip")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File is not a zip");
        }

        if (file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File is empty");
        }

        Path tempFile = Files.createTempFile("iotfsdb-", ".zip");
        try {
            file.transferTo(tempFile);
            importService.importData(tempFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }

    }

}
