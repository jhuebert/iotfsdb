package org.huebert.iotfsdb.series;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Immutable definition of a series")
public class SeriesDefinition {

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

}
