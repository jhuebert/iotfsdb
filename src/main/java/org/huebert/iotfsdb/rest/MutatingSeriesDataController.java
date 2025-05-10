package org.huebert.iotfsdb.rest;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.InsertRequest;
import org.huebert.iotfsdb.service.ImportService;
import org.huebert.iotfsdb.service.InsertService;
import org.huebert.iotfsdb.service.ParallelUtil;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
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

import static org.springframework.http.HttpStatus.NO_CONTENT;

@Validated
@Slf4j
@RestController
@RequestMapping("/v2/data")
@ConditionalOnProperty(prefix = "iotfsdb", value = "read-only", havingValue = "false")
@PreAuthorize("hasRole('API_WRITE')")
public class MutatingSeriesDataController {

    private final InsertService insertService;

    private final ImportService importService;

    public MutatingSeriesDataController(@NotNull InsertService insertService, @NotNull ImportService importService) {
        this.insertService = insertService;
        this.importService = importService;
    }

    @Operation(tags = "Data", summary = "Bulk insert of data")
    @PostMapping
    @ResponseStatus(NO_CONTENT)
    public void insert(@NotNull @Valid @RequestBody List<InsertRequest> request) {
        ParallelUtil.forEach(request, insertService::insert);
    }

    @Operation(tags = "Data", summary = "Imports a database archive")
    @PostMapping("import")
    @ResponseStatus(NO_CONTENT)
    public void importSeries(@NotNull @Valid @RequestParam("file") MultipartFile file) throws IOException {

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
