package org.huebert.iotfsdb.series;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
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
    private String id;

    @NotNull
    private SeriesType type;

    @NotNull
    @Min(1)
    @Max(86400)
    private int interval;

    @NotNull
    private PartitionPeriod partition;

}
