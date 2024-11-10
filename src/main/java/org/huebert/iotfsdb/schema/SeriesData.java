package org.huebert.iotfsdb.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Single data value")
public class SeriesData {

    @Schema(description = "Date and time of the value")
    @NotNull
    private ZonedDateTime time;

    @Schema(description = "Value that is associated with the specified date and time. The value should match the series NumberType and can be null.")
    private Number value;

}
