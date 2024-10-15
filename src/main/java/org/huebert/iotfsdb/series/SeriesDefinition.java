package org.huebert.iotfsdb.series;

import org.apache.logging.log4j.util.Strings;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public record SeriesDefinition(
    String id,
    SeriesType type,
    int interval,
    PartitionPeriod partition
) {

    public static void checkValid(SeriesDefinition seriesDefinition) {

        if (seriesDefinition == null) {
            throw new IllegalArgumentException("series is null");
        }

        if (Strings.isBlank(seriesDefinition.id)) {
            throw new IllegalArgumentException("id is blank");
        }

        if (seriesDefinition.type == null) {
            throw new IllegalArgumentException("type is null");
        }

        if (seriesDefinition.partition == null) {
            throw new IllegalArgumentException("partition period is null");
        }

        if (seriesDefinition.interval < 1) {
            throw new IllegalArgumentException("value interval must be at least 1");
        }

        if (seriesDefinition.interval > Duration.of(1, ChronoUnit.DAYS).getSeconds()) {
            throw new IllegalArgumentException("value interval must be no more than one day");
        }

    }

}
