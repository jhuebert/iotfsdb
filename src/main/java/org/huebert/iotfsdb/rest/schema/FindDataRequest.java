package org.huebert.iotfsdb.rest.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Range;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.huebert.iotfsdb.series.Reducer;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Data search request")
public class FindDataRequest {

    @Schema(description = "Earliest date and time that values should be have")
    @NotNull
    private ZonedDateTime from;

    @Schema(description = "Latest date and time that values should be have", defaultValue = "Current date and time")
    @NotNull
    private ZonedDateTime to = ZonedDateTime.now();

    @Schema(description = "Regular expression that is used to match series IDs", defaultValue = ".*")
    @NotNull
    private Pattern pattern = Pattern.compile(".*");

    @Schema(description = "Key and values that series metadata must contain")
    @NotNull
    private Map<String, String> metadata = new HashMap<>();

    @Schema(description = "Interval in seconds of the returned data for each series")
    @Positive
    private Integer interval;

    @Schema(description = "Maximum number of points to return for each series")
    @Positive
    private Integer size;

    @Schema(description = "Indicates whether to include null values in the list of values for a series", defaultValue = "false")
    private boolean includeNull = false;

    @Schema(description = "Indicates whether to use BigDecimal for mathematical operations", defaultValue = "false")
    private boolean useBigDecimal = false;

    @Schema(description = "Value to use in place of null in a series", defaultValue = "null")
    private Number nullValue = null;

    @Schema(description = "Reducing function used to produce a single value from a series for a given time period", defaultValue = "AVERAGE")
    @NotNull
    private Reducer timeReducer = Reducer.AVERAGE;

    @Schema(description = "Reducing function used to produce a single value for a given time period for all series. This results in a single series in the response named \"reduced\"")
    private Reducer seriesReducer;

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
