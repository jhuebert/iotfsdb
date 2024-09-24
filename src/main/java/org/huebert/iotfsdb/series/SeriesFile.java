package org.huebert.iotfsdb.series;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Series details")
public class SeriesFile {

    @Schema(description = "Immutable definition of a series")
    @NotNull
    SeriesDefinition definition;

    @Schema(description = "Series key and value pairs")
    @NotNull
    private Map<String, String> metadata;

}
