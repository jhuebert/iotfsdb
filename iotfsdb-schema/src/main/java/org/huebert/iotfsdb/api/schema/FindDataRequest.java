package org.huebert.iotfsdb.api.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Range;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Data search request")
public class FindDataRequest {

    @Schema(description = "Relative time range to use in place of an explicitly provided from and to time. This takes priority over from and to if they are also specified.")
    private DateTimePreset dateTimePreset;

    @Schema(description = "Timezone that the output data should use. The timezone of from will be used if not specified. UTC will be used in the event that the timezone is not specified with a time preset.")
    private TimeZone timezone;

    @Schema(description = "Earliest date and time that values should be have")
    private ZonedDateTime from;

    @Schema(description = "Latest date and time that values should be have", defaultValue = "Current date and time")
    private ZonedDateTime to = ZonedDateTime.now();

    @Schema(description = "Properties used to match series", defaultValue = ".*")
    @NotNull
    private FindSeriesRequest series = new FindSeriesRequest();

    @Schema(description = "Interval in milliseconds of the returned data for each series")
    @Positive
    private Long interval;

    @Schema(description = "Maximum number of points to return for each series")
    @Positive
    private Integer size;

    @Schema(description = "Indicates whether to include null values in the list of values for a series", defaultValue = "false")
    private boolean includeNull = false;

    @Schema(description = "Indicates whether to use BigDecimal for mathematical operations", defaultValue = "false")
    private boolean useBigDecimal = false;

    @Schema(description = "Indicates whether to return the previous non-null value when a null value is encountered", defaultValue = "false")
    private boolean usePrevious = false;

    @Schema(description = "Value to use in place of null in a series", defaultValue = "null")
    private Number nullValue = null;

    @Schema(description = "Reducing function used to produce a single value from a series for a given time period", defaultValue = "AVERAGE")
    @NotNull
    private Reducer timeReducer = Reducer.AVERAGE;

    @Schema(description = "Reducing function used to produce a single value for a given time period for all series. This results in a single series in the response named \"reduced\"")
    private Reducer seriesReducer;

    @JsonIgnore
    @AssertTrue
    public boolean isRangeValid() {
        if ((dateTimePreset != null) && (dateTimePreset != DateTimePreset.NONE)) {
            return true;
        }
        return from != null && to != null && to.isAfter(from);
    }

    @JsonIgnore
    public Range<ZonedDateTime> getRange() {
        if ((dateTimePreset != null) && (dateTimePreset != DateTimePreset.NONE)) {
            ZoneId zoneId = timezone == null ? ZoneOffset.UTC : timezone.toZoneId();
            ZonedDateTime now = ZonedDateTime.now().withZoneSameInstant(zoneId);
            return Range.closed(now.minus(dateTimePreset.getDuration()), now);
        }
        ZoneId zoneId = timezone == null ? from.getZone() : timezone.toZoneId();
        return Range.closed(from.withZoneSameInstant(zoneId), to.withZoneSameInstant(zoneId));
    }

    @JsonIgnore
    public Predicate<SeriesData> getNullPredicate() {
        return isIncludeNull() ? _ -> true : seriesData -> seriesData.getValue() != null;
    }

    @JsonIgnore
    public Consumer<SeriesData> getPreviousConsumer() {
        if (!isUsePrevious()) {
            return _ -> {
            };
        }
        AtomicReference<Number> previous = new AtomicReference<>(null);
        return seriesData -> {
            Number value = seriesData.getValue();
            if (value != null) {
                previous.set(value);
            } else {
                seriesData.setValue(previous.get());
            }
        };
    }

}
