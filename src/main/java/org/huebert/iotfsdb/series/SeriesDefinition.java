package org.huebert.iotfsdb.series;

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
public class SeriesDefinition {

    @NotBlank
    @Pattern(regexp = "[a-z0-9][a-z0-9._-]{0,127}")
    private String id;

    @NotNull
    private NumberType type;

    @NotNull
    @Min(1)
    @Max(86400)
    private int interval;

    @NotNull
    private PartitionPeriod partition;

}
