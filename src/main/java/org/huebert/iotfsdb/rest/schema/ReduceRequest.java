package org.huebert.iotfsdb.rest.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Range;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.huebert.iotfsdb.series.Reducer;

import java.time.ZonedDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReduceRequest {

    @NotNull
    private ZonedDateTime from;

    @NotNull
    private ZonedDateTime to;

    @NotNull
    @Min(1)
    @Max(86400)
    private int interval; // TODO Should error if new interval is smaller than the previous

    private Reducer reducer;

    @JsonIgnore
    public Range<ZonedDateTime> getRange() {
        return Range.closed(from, to);
    }

}
