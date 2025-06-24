package org.huebert.iotfsdb.api.rest;

import com.google.common.net.HttpHeaders;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindDataResponse;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.service.ExportService;
import org.huebert.iotfsdb.service.QueryService;
import org.huebert.iotfsdb.service.TimeConverter;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Validated
@Slf4j
@RestController
@RequestMapping("/v2/data")
public class SeriesDataController {

    private static final MediaType ZIP_MEDIA_TYPE = MediaType.parseMediaType("application/zip");

    private final ExportService exportService;

    private final QueryService queryService;

    public SeriesDataController(ExportService exportService, QueryService queryService) {
        this.exportService = exportService;
        this.queryService = queryService;
    }

    @CaptureStats(
        group = "rest-v2", type = "data", operation = "find", javaClass = SeriesDataController.class, javaMethod = "findData",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/data/find"),
        }
    )
    @Operation(tags = "Data", summary = "Finds data matching the input request")
    @PostMapping("find")
    public List<FindDataResponse> findData(@Valid @RequestBody FindDataRequest request) {
        return queryService.findData(request);
    }

    @CaptureStats(
        group = "rest-v2", type = "data", operation = "export", javaClass = SeriesDataController.class, javaMethod = "exportData",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/data/export"),
        }
    )
    @Operation(tags = "Data", summary = "Exports a database archive of matching series")
    @PostMapping(value = "export", produces = "application/zip")
    public ResponseEntity<StreamingResponseBody> exportData(@Valid @RequestBody FindSeriesRequest request) {
        String formattedTime = TimeConverter.toUtc(ZonedDateTime.now()).format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        return ResponseEntity.ok()
            .header(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=iotfsdb-" + formattedTime + ".zip")
            .contentType(ZIP_MEDIA_TYPE)
            .body(out -> exportService.export(request, out));
    }

}
