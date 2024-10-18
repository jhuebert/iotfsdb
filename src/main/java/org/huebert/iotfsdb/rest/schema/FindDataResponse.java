package org.huebert.iotfsdb.rest.schema;

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
public class FindDataResponse {

    @NotBlank
    private String series;

    @NotNull
    private Map<String, String> metadata;

    @NotNull
    private List<SeriesData> data;
}
