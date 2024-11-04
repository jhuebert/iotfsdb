package org.huebert.iotfsdb.rest.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Series search request")
public class FindSeriesRequest {

    @Schema(description = "Regular expression that is used to match series IDs", defaultValue = ".*")
    @NotNull
    private Pattern pattern = Pattern.compile(".*");

    @Schema(description = "Key and values that matching series metadata must contain")
    @NotNull
    private Map<String, Pattern> metadata = new HashMap<>();

}
