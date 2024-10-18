package org.huebert.iotfsdb.rest.schema;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FindSeriesRequest {

    @NotNull
    private Pattern pattern = Pattern.compile(".*");

    @NotNull
    private Map<String, String> metadata = new HashMap<>();

}
