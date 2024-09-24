package org.huebert.iotfsdb.util;

import com.google.common.collect.Range;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class TimeUtil {

    public static ZonedDateTime min(ZonedDateTime a, ZonedDateTime b) {
        if ((a == null) || (b == null)) {
            if (a != null) {
                return a;
            }
            return b;
        }
        return a.compareTo(b) <= 0 ? a : b;
    }

    public static ZonedDateTime max(ZonedDateTime a, ZonedDateTime b) {
        if ((a == null) || (b == null)) {
            if (a != null) {
                return a;
            }
            return b;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    public static Range<LocalDateTime> convertToUtc(Range<ZonedDateTime> zonedRange) {
        return Range.range(
            convertToUtc(zonedRange.lowerEndpoint()),
            zonedRange.lowerBoundType(),
            convertToUtc(zonedRange.upperEndpoint()),
            zonedRange.upperBoundType()
        );
    }

    public static LocalDateTime convertToUtc(ZonedDateTime zonedDateTime) {
        return zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }
}
