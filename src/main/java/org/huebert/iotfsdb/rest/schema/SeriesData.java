package org.huebert.iotfsdb.rest.schema;

import java.util.List;

public record SeriesData(
    Series series,
    List<?> data
) {
}
