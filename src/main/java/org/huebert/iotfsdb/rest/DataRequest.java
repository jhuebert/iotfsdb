package org.huebert.iotfsdb.rest;

import lombok.Data;
import org.huebert.iotfsdb.series.Aggregation;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.regex.Pattern;

@Data
public class DataRequest {
    private final ZonedDateTime start;
    private final ZonedDateTime end;
    private final Pattern pattern;
    private final Integer interval;
    private final Integer maxSize;
    private final boolean includeNull;
    private final Aggregation aggregation1;
    private final Aggregation aggregation2;
    private final Map<String, String> metadata;
}
