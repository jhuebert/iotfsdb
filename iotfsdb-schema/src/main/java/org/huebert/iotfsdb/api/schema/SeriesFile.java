package org.huebert.iotfsdb.api.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Singular;

import java.util.Map;

@Data
@Builder
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Series details")
public class SeriesFile {

    @Schema(description = "Immutable definition of a series")
    @NotNull
    private SeriesDefinition definition;

    @Singular(value = "metadata")
    @EqualsAndHashCode.Exclude
    @Schema(description = "Series key and value pairs")
    @NotNull
    private Map<String, String> metadata;

    @JsonIgnore
    public String getId() {
        return definition.getId();
    }

}
