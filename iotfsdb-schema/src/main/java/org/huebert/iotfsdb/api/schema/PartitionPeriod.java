package org.huebert.iotfsdb.api.schema;

import lombok.Getter;

import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.regex.Pattern;

public enum PartitionPeriod {

    DAY(Period.ofDays(1), DateTimeFormatter.ofPattern("yyyyMMdd"), Pattern.compile("\\d{8}")),

    MONTH(Period.ofMonths(1), DateTimeFormatter.ofPattern("yyyyMM"), Pattern.compile("\\d{6}")),

    YEAR(Period.ofYears(1), DateTimeFormatter.ofPattern("yyyy"), Pattern.compile("\\d{4}"));

    @Getter
    private final Period period;

    private final DateTimeFormatter formatter;

    private final Pattern pattern;

    PartitionPeriod(Period period, DateTimeFormatter formatter, Pattern pattern) {
        this.period = period;
        this.formatter = formatter;
        this.pattern = pattern;
    }

    public boolean matches(String filename) {
        return pattern.matcher(filename).matches();
    }

    public LocalDateTime getStart(LocalDateTime dateTime) {
        int year = dateTime.getYear();
        int month = this == YEAR ? 1 : dateTime.getMonthValue();
        int day = this == DAY ? dateTime.getDayOfMonth() : 1;
        return LocalDateTime.of(year, month, day, 0, 0, 0);
    }

    public String getFilename(LocalDateTime dateTime) {
        return formatter.format(dateTime);
    }

    public LocalDateTime parseStart(String filename) {
        TemporalAccessor parsed = formatter.parse(filename);
        int year = parsed.get(ChronoField.YEAR);
        int month = this == YEAR ? 1 : parsed.get(ChronoField.MONTH_OF_YEAR);
        int day = this == DAY ? parsed.get(ChronoField.DAY_OF_MONTH) : 1;
        return LocalDateTime.of(year, month, day, 0, 0, 0);
    }

}
