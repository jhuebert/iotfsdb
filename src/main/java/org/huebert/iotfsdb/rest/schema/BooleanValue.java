package org.huebert.iotfsdb.rest.schema;

public record BooleanValue(
    Long timestamp,
    Boolean value //Need to allow for null value
) {

}
