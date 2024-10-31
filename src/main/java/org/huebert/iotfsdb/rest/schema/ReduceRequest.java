package org.huebert.iotfsdb.rest.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Range;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
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
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Request to reduce series data. Only partitions that are entirely enclosed by the input date and time range will be reduced.")
public class ReduceRequest {

    @Schema(description = "Earliest date and time that should be reduced")
    @NotNull
    private ZonedDateTime from;

    @Schema(description = "Latest date and time that should be reduced")
    @NotNull
    private ZonedDateTime to;

    @Schema(description = "Interval in seconds of the resulting reduced data")
    @NotNull
    @Min(1)
    @Max(86400)
    private int interval;

    @Schema(description = "Reducing function used to produce a single value for a series for a given time period", defaultValue = "AVERAGE")
    private Reducer reducer;

    @JsonIgnore
    public Range<ZonedDateTime> getRange() {
        return Range.closed(from, to);
    }

    @AssertTrue
    private boolean isValid() {
        if ((from == null) || (to == null)) {
            return false;
        }
        if (to.compareTo(from) <= 0) {
            return false;
        }
        return true;
    }

}
