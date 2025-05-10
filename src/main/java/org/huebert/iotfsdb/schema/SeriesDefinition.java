package org.huebert.iotfsdb.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.*;
import lombok.*;

import java.time.Duration;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

@Data
@Builder
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Immutable definition of a series")
public class SeriesDefinition {

    public static final String ID_PATTERN = "[a-z0-9][a-z0-9._-]{0,127}";

    private static final Set<NumberType> TYPES_WITH_RANGE = EnumSet.of(NumberType.CURVED1, NumberType.CURVED2, NumberType.CURVED4, NumberType.MAPPED1, NumberType.MAPPED2, NumberType.MAPPED4);

    private static final Map<PartitionPeriod, Long> MAX_INTERVALS = new EnumMap<>(PartitionPeriod.class);

    static {
        MAX_INTERVALS.put(PartitionPeriod.DAY, Duration.ofDays(1).toMillis());
        MAX_INTERVALS.put(PartitionPeriod.MONTH, Duration.ofDays(28).toMillis());
        MAX_INTERVALS.put(PartitionPeriod.YEAR, Duration.ofDays(365).toMillis());
    }

    @Schema(description = "Series ID")
    @NotBlank
    @Pattern(regexp = ID_PATTERN)
    private String id;

    @Schema(description = "Data type of the numbers stored for this series.")
    @NotNull
    private NumberType type;

    @Schema(description = "Minimum time interval in milliseconds that the series will contain. The interval should exactly divide a day with no remainder.")
    @NotNull
    @Positive
    private Long interval;

    @Schema(description = "Time period of data contained in a single partition file.")
    @NotNull
    private PartitionPeriod partition;

    @Schema(description = "Minimum supported value when using a mapped range type. Values to be stored will be constrained to this minimum value.")
    private Double min;

    @Schema(description = "Maximum supported value when using a mapped range type. Values to be stored will be constrained to this maximum value.")
    private Double max;

    @JsonIgnore
    @AssertTrue
    public boolean isTypeValid() {
        if ((min == null) != (max == null)) {
            return false;
        } else if (TYPES_WITH_RANGE.contains(type)) {
            return min != null && min < max;
        }
        return min == null;
    }

    @JsonIgnore
    @AssertTrue
    public boolean isIntervalValid() {
        Long maxInterval = MAX_INTERVALS.get(partition);
        return maxInterval != null && interval <= maxInterval;
    }

    @JsonIgnore
    public Duration getIntervalDuration() {
        return Duration.ofMillis(interval);
    }

}
