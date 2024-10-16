package org.huebert.iotfsdb.rest;

import com.google.common.collect.Range;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.huebert.iotfsdb.series.Aggregation;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Data
@NoArgsConstructor
public class DataRequest {

    @NotNull
    private ZonedDateTime start;

    @NotNull
    private ZonedDateTime end = ZonedDateTime.now();

    @NotNull
    private Pattern pattern = Pattern.compile(".*");

    @Positive
    private Integer interval;

    @Positive
    private Integer maxSize;

    private boolean includeNull = false;

    @NotNull
    private Aggregation aggregation1 = Aggregation.AVERAGE;

    private Aggregation aggregation2;

    @NotNull
    private Map<String, String> metadata = new HashMap<>();

    public Range<ZonedDateTime> getRange() {
        return Range.closed(start, end);
    }

}
