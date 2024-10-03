package org.huebert.iotfsdb.schema;

import java.time.ZonedDateTime;

public record DataValue(
    ZonedDateTime dateTime,
    String value
) {

    public static void checkValid(DataValue dataValue) {

        if (dataValue.dateTime == null) {
            throw new IllegalArgumentException("date and time is null");
        }

    }
}
