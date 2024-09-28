package org.huebert.iotfsdb.schema;

import java.time.Duration;

public record Series(
    String id,
    Duration valueInterval,
    FileInterval fileInterval,
    SeriesType type
) {

}
