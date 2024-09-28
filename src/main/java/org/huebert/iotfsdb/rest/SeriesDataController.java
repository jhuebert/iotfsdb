package org.huebert.iotfsdb.rest;

import com.google.common.collect.Range;
import org.huebert.iotfsdb.service.SeriesService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/v1/data")
public class SeriesDataController {

    private final SeriesService seriesService;

    public SeriesDataController(SeriesService seriesService) {
        this.seriesService = seriesService;
    }

    @GetMapping
    public Map<String, Map<LocalDateTime, ?>> findData(
        @RequestParam(name = "pattern", required = false, defaultValue = ".*") String pattern,
        @RequestParam(name = "start") LocalDateTime start,
        @RequestParam(name = "end", required = false) LocalDateTime end,
        @RequestParam(name = "interval", required = false, defaultValue = "1") int interval,
        @RequestParam(name = "includeNull", required = false, defaultValue = "false") boolean includeNull,
        @RequestParam Map<String, String> metadata
    ) {
        Map<String, String> trimmedMetadata = new HashMap<>(metadata);
        trimmedMetadata.keySet().removeAll(Set.of("pattern", "start", "end", "interval", "includeNull"));
        Range<LocalDateTime> range = Range.closed(start, end == null ? LocalDateTime.now() : end);
        return seriesService.get(pattern, trimmedMetadata, range, interval, includeNull);
    }

}
