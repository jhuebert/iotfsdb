package org.huebert.iotfsdb.series;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.EnumSet;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Immutable definition of a series")
public class SeriesDefinition {

    private static final EnumSet<NumberType> TYPES_WITH_RANGE = EnumSet.of(NumberType.CURVED1, NumberType.CURVED2, NumberType.CURVED4, NumberType.MAPPED1, NumberType.MAPPED2, NumberType.MAPPED4);

    @Schema(description = "Series ID")
    @NotBlank
    @Pattern(regexp = "[a-z0-9][a-z0-9._-]{0,127}")
    private String id;

    @Schema(description = "Data type of the numbers stored for this series.")
    @NotNull
    private NumberType type;

    @Schema(description = "Minimum time interval in milliseconds that the series will contain. The interval should exactly divide a day with no remainder.")
    @NotNull
    @Min(1)
    @Max(86400000)
    private int interval;

    @Schema(description = "Time period of data contained in a single partition file.")
    @NotNull
    private PartitionPeriod partition;

    @Schema(description = "Minimum supported value when using a mapped range type. Values to be stored will be constrained to this minimum value.")
    private Double min;

    @Schema(description = "Maximum supported value when using a mapped range type. Values to be stored will be constrained to this maximum value.")
    private Double max;

    @AssertTrue
    private boolean isValid() {
        if ((type != null) && TYPES_WITH_RANGE.contains(type)) {
            if ((min == null) || (max == null)) {
                return false;
            }
            if (max <= min) {
                return false;
            }
        } else if ((min != null) || (max != null)) {
            return false;
        }
        return true;
    }
}
