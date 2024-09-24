package org.huebert.iotfsdb.rest.schema;

import java.util.List;

public record FloatSeriesData<T>(
    Series series,
    List<FloatValue> data
) {
}
