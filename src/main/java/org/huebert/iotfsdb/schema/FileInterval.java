package org.huebert.iotfsdb.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public enum FileInterval {

    DAY(Period.ofDays(1), DateTimeFormatter.ofPattern("yyyyMMdd")),
    MONTH(Period.ofMonths(1), DateTimeFormatter.ofPattern("yyyyMM")),
    YEAR(Period.ofYears(1), DateTimeFormatter.ofPattern("yyyy"));

    private final Period period;

    private final DateTimeFormatter formatter;

    FileInterval(Period period, DateTimeFormatter formatter) {
        this.period = period;
        this.formatter = formatter;
    }

    public static FileInterval findMatchingInterval(String filename) {
        for (FileInterval fileInterval : values()) {
            if (fileInterval.formatter.toString().length() == filename.length()) {
                return fileInterval;
            }
        }
        return null;
    }

    public String getFilename(LocalDateTime dateTime) {
        return formatter.format(dateTime);
    }

    public LocalDateTime getStart(LocalDateTime dateTime) {
        return getStart(getFilename(dateTime));
    }

    public LocalDateTime getStart(String filename) {
        TemporalAccessor parsed = formatter.parse(filename);

        int year = parsed.get(ChronoField.YEAR);

        int month = 1;
        if (parsed.isSupported(ChronoField.MONTH_OF_YEAR)) {
            month = parsed.get(ChronoField.MONTH_OF_YEAR);
        }

        int day = 1;
        if (parsed.isSupported(ChronoField.DAY_OF_MONTH)) {
            day = parsed.get(ChronoField.DAY_OF_MONTH);
        }

        int hour = 0;
        if (parsed.isSupported(ChronoField.HOUR_OF_DAY)) {
            hour = parsed.get(ChronoField.HOUR_OF_DAY);
        }

        int minute = 0;
        if (parsed.isSupported(ChronoField.MINUTE_OF_HOUR)) {
            minute = parsed.get(ChronoField.MINUTE_OF_HOUR);
        }

        int second = 0;
        if (parsed.isSupported(ChronoField.SECOND_OF_MINUTE)) {
            second = parsed.get(ChronoField.SECOND_OF_MINUTE);
        }

        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    public LocalDateTime getEnd(LocalDateTime start) {
        return start.plus(period);
    }

    public Duration getDuration(LocalDateTime start) {
        return Duration.between(start, getEnd(start));
    }

    public Range<LocalDateTime> getRange(LocalDateTime start) {
        return Range.closedOpen(start, getEnd(start));
    }

    public int calculateSize(LocalDateTime start, Duration valueInterval) {
        int size = (int) getDuration(start).dividedBy(valueInterval);
        Preconditions.checkArgument(size > 0);
        return size;
    }

}
