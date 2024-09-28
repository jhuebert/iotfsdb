package org.huebert.iotfsdb.rest.schema;

import java.util.Map;
import java.util.Set;

public record SeriesMetadata(
    Set<String> tags,
    Map<String, String> properties
) {

}
