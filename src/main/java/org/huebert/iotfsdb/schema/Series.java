package org.huebert.iotfsdb.schema;

public record Series(
    String id,
    SeriesType type,
    int valueInterval,
    FileInterval fileInterval
) {

}
