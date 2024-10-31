package org.huebert.iotfsdb.rest.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Data search results")
public class FindDataResponse {

    @Schema(description = "Series ID for the matching data")
    @NotBlank
    private String series;

    @Schema(description = "Series metadata")
    @NotNull
    private Map<String, String> metadata;

    @Schema(description = "Data that matches the search request")
    @NotNull
    private List<SeriesData> data;

}
