package org.huebert.iotfsdb.api.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

import static org.huebert.iotfsdb.api.schema.SeriesDefinition.ID_PATTERN;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Batch insert request for a series")
public class InsertRequest {

    @Schema(description = "Series ID that should be used to insert the values")
    @Pattern(regexp = ID_PATTERN)
    @NotBlank
    private String series;

    @Schema(description = "Series data that should be inserted")
    @NotEmpty
    private List<SeriesData> values;

    @Schema(description = "Reducer that handles situations where there is a non-null value that already exists. The default behavior is to overwrite any preexisting data.")
    private Reducer reducer;

}
