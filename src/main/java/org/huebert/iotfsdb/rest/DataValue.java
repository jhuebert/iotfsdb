package org.huebert.iotfsdb.rest;

import java.time.ZonedDateTime;

public record DataValue(
    ZonedDateTime dateTime,
    String value
) {

    public static void checkValid(DataValue dataValue) {

        if (dataValue == null) {
            throw new IllegalArgumentException("data value is null");
        }

        if (dataValue.dateTime == null) {
            throw new IllegalArgumentException("date and time is null");
        }

    }
}
