package org.huebert.iotfsdb.rest.schema;

import java.util.Map;

public record SeriesData(
    Series series,
    Map<Long, Float> data
) {
}
