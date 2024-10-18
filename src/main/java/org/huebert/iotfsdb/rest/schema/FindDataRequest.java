package org.huebert.iotfsdb.rest.schema;

import com.google.common.collect.Range;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.huebert.iotfsdb.series.Reducer;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FindDataRequest {

    @NotNull
    private ZonedDateTime from;

    @NotNull
    private ZonedDateTime to = ZonedDateTime.now();

    @NotNull
    private Pattern pattern = Pattern.compile(".*");

    @Positive
    private Integer interval;

    @Positive
    private Integer size;

    private boolean includeNull = false;

    @NotNull
    private Reducer timeReducer = Reducer.AVERAGE;

    private Reducer seriesReducer;

    @NotNull
    private Map<String, String> metadata = new HashMap<>();

    public Range<ZonedDateTime> getRange() {
        return Range.closed(from, to);
    }

}
