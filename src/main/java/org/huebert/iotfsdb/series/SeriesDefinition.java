package org.huebert.iotfsdb.series;

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
@Schema(description = "Immutable definition of a series")
public class SeriesDefinition {

    private static final EnumSet<NumberType> FIXED = EnumSet.of(NumberType.FIXED1, NumberType.FIXED2, NumberType.FIXED4);

    @Schema(description = "Series ID")
    @NotBlank
    @Pattern(regexp = "[a-z0-9][a-z0-9._-]{0,127}")
    private String id;

    @Schema(description = "Data type of the numbers stored for this series.")
    @NotNull
    private NumberType type;

    @Schema(description = "Minimum time interval that the series will contain.")
    @NotNull
    @Min(1)
    @Max(86400)
    private int interval;

    @Schema(description = "Time period of data contained in a single partition file.")
    @NotNull
    private PartitionPeriod partition;

    @Schema(description = "Minimum supported value when using a fixed range type. Values to be stored will be constrained to this value.")
    private Double min;

    @Schema(description = "Maximum supported value when using a fixed range type. Values to be stored will be constrained to this value.")
    private Double max;

    @AssertTrue(message = "Values are invalid")
    private boolean isValid() {
        if ((type != null) && FIXED.contains(type)) {
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
