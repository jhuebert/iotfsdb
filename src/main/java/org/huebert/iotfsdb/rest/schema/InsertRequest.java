package org.huebert.iotfsdb.rest.schema;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Batch insert request for a series")
public class InsertRequest {

    @Schema(description = "Series ID that should be used to insert the values")
    @NotBlank
    private String series;

    @Schema(description = "Series ID that should be used to insert the values")
    @NotEmpty
    private List<SeriesData> values;

}
