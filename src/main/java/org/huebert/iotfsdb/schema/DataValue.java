package org.huebert.iotfsdb.schema;

import java.time.LocalDateTime;

public record DataValue(
    LocalDateTime dateTime,
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
