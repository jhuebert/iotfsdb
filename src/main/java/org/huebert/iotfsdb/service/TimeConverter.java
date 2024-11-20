package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class TimeConverter {

    public static Range<LocalDateTime> toUtc(Range<ZonedDateTime> zonedRange) {
        return Range.range(
            toUtc(zonedRange.lowerEndpoint()),
            zonedRange.lowerBoundType(),
            toUtc(zonedRange.upperEndpoint()),
            zonedRange.upperBoundType()
        );
    }

    public static LocalDateTime toUtc(ZonedDateTime zonedDateTime) {
        return zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

}
