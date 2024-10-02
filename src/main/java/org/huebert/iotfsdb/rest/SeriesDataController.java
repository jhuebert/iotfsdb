package org.huebert.iotfsdb.rest;

import com.google.common.collect.Range;
import org.huebert.iotfsdb.series.SeriesAggregation;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

@RestController
@RequestMapping("/v1/data")
public class SeriesDataController {

    private static final Set<String> PARAMS = Set.of("pattern", "start", "end", "interval", "includeNull", "aggregation");

    private final SeriesService seriesService;

    public SeriesDataController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @GetMapping
    public Map<String, Map<LocalDateTime, ?>> findData(
        @RequestParam(name = "pattern", required = false, defaultValue = ".*") Pattern pattern,
        @RequestParam(name = "start") LocalDateTime start,
        @RequestParam(name = "end") LocalDateTime end,
        @RequestParam(name = "interval", required = false, defaultValue = "1") int interval,
        @RequestParam(name = "includeNull", required = false, defaultValue = "false") boolean includeNull,
        @RequestParam(name = "aggregation", required = false, defaultValue = "AVERAGE") SeriesAggregation aggregation,
        @RequestParam Map<String, String> metadata
    ) {

        if (metadata == null) {
            throw new ResponseStatusException(BAD_REQUEST, "metadata is null");
        }

        if (pattern == null) {
            throw new ResponseStatusException(BAD_REQUEST, "pattern is null");
        }

        if (start == null) {
            throw new ResponseStatusException(BAD_REQUEST, "start is null");
        }

        if (end == null) {
            throw new ResponseStatusException(BAD_REQUEST, "end is null");
        }

        if (aggregation == null) {
            throw new ResponseStatusException(BAD_REQUEST, "aggregation is null");
        }

        if (end.isBefore(start)) {
            throw new ResponseStatusException(BAD_REQUEST, "end is before start");
        }

        if (interval < 0) {
            throw new ResponseStatusException(BAD_REQUEST, "interval must be at least 1");
        }

        Map<String, String> trimmedMetadata = new HashMap<>(metadata);
        trimmedMetadata.keySet().removeAll(PARAMS);

        Range<LocalDateTime> range = Range.closed(start, end);

        return seriesService.get(pattern, trimmedMetadata, range, interval, includeNull, aggregation);
    }

}
