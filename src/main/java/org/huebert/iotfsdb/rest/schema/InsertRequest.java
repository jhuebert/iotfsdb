package org.huebert.iotfsdb.rest.schema;

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
public class InsertRequest {

    @NotBlank
    private String series;

    @NotEmpty
    private List<SeriesData> values;

}
