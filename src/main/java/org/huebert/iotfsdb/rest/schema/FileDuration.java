package org.huebert.iotfsdb.rest.schema;

import lombok.Getter;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

@Getter
public enum FileDuration {

    SECOND(ChronoUnit.SECONDS, DateTimeFormatter.ofPattern("yyyyMMddHHmmss")),
    MINUTE(ChronoUnit.MINUTES, DateTimeFormatter.ofPattern("yyyyMMddHHmm")),
    HOUR(ChronoUnit.HOURS, DateTimeFormatter.ofPattern("yyyyMMddHH")),
    DAY(ChronoUnit.DAYS, DateTimeFormatter.ofPattern("yyyyMMdd")),
    MONTH(ChronoUnit.MONTHS, DateTimeFormatter.ofPattern("yyyyMM")),
    YEAR(ChronoUnit.YEARS, DateTimeFormatter.ofPattern("yyyy"));

    private final Duration duration;

    private final DateTimeFormatter formatter;

    FileDuration(ChronoUnit unit, DateTimeFormatter formatter) {
        this.duration = Duration.of(1, unit);
        this.formatter = formatter;
    }

    public String getFilename(LocalDateTime dateTime) {
        return formatter.format(dateTime);
    }

}
