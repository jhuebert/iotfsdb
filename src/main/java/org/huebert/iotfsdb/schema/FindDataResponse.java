package org.huebert.iotfsdb.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Data search results")
public class FindDataResponse {

    @Schema(description = "Series for the matching data")
    @NotNull
    private SeriesFile series;

    @Schema(description = "Data that matches the search request")
    @NotNull
    private List<SeriesData> data;

}
