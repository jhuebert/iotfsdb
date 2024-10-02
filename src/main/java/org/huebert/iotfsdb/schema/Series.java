package org.huebert.iotfsdb.schema;

import org.apache.logging.log4j.util.Strings;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public record Series(
    String id,
    SeriesType type,
    int valueInterval,
    FileInterval fileInterval
) {

    public int calculateSize(LocalDateTime start) {
        Duration duration = Duration.of(valueInterval, ChronoUnit.SECONDS);
        return fileInterval.calculateSize(start, duration);
    }

    public static void checkValid(Series series) {

        if (series == null) {
            throw new IllegalArgumentException("series is null");
        }

        if (series.id == null) {
            throw new IllegalArgumentException("id is null");
        }

        if (Strings.isBlank(series.id)) {
            throw new IllegalArgumentException("id is blank");
        }

        if (series.type == null) {
            throw new IllegalArgumentException("type is null");
        }

        if (series.fileInterval == null) {
            throw new IllegalArgumentException("file interval is null");
        }

        if (series.valueInterval < 1) {
            throw new IllegalArgumentException("value interval must be at least 1");
        }

        if (series.valueInterval > Duration.of(1, ChronoUnit.DAYS).getSeconds()) {
            throw new IllegalArgumentException("value interval must be no more than one day");
        }

    }

}
