package org.huebert.iotfsdb.rest.schema;

import java.time.Duration;

public record Series(
    String id,
    Duration valueDuration,
    FileDuration fileDuration,
    SeriesType type
//    Map<String, String> properties
) {

    //TODO value interval must be less than file interval

}
