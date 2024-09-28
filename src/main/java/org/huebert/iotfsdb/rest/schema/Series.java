package org.huebert.iotfsdb.rest.schema;

import java.time.Duration;

public record Series(
    String id,
    Duration valueInterval,
    FileInterval fileInterval,
    SeriesType type
) {

}
