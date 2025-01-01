package org.huebert.iotfsdb.schema;

import lombok.Getter;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;

@Getter
public enum DateTimePreset {

    NONE(null),
    LAST_5_MINUTES(Duration.ofMinutes(5)),
    LAST_15_MINUTES(Duration.ofMinutes(15)),
    LAST_30_MINUTES(Duration.ofMinutes(30)),
    LAST_1_HOUR(Duration.ofHours(1)),
    LAST_3_HOURS(Duration.ofHours(3)),
    LAST_6_HOURS(Duration.ofHours(6)),
    LAST_12_HOURS(Duration.ofHours(12)),
    LAST_24_HOURS(Duration.ofHours(24)),
    LAST_2_DAYS(Period.ofDays(2)),
    LAST_7_DAYS(Period.ofDays(7)),
    LAST_30_DAYS(Period.ofDays(30)),
    LAST_90_DAYS(Period.ofDays(90)),
    LAST_6_MONTHS(Period.ofMonths(6)),
    LAST_1_YEAR(Period.ofYears(1)),
    LAST_2_YEARS(Period.ofYears(2)),
    LAST_5_YEARS(Period.ofYears(5)),
    ;

    private final TemporalAmount duration;

    DateTimePreset(TemporalAmount duration) {
        this.duration = duration;
    }

}
