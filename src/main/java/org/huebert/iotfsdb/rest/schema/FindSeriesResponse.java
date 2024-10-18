package org.huebert.iotfsdb.rest.schema;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.huebert.iotfsdb.series.SeriesDefinition;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FindSeriesResponse {

    @NotNull
    private SeriesDefinition definition;

    @NotNull
    private Map<String, String> metadata;
}
