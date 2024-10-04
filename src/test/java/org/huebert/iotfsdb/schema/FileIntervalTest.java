package org.huebert.iotfsdb.schema;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class FileIntervalTest {


    @Test
    public void testFindMatch() {
        assertThat(FileInterval.findMatch("2024")).isEqualTo(FileInterval.YEAR);
        assertThat(FileInterval.findMatch("202410")).isEqualTo(FileInterval.MONTH);
        assertThat(FileInterval.findMatch("20241003")).isEqualTo(FileInterval.DAY);
        assertThat(FileInterval.findMatch("202")).isEqualTo(null);
    }

    @Test
    public void testGetFilename() {
        LocalDateTime dateTime = LocalDateTime.parse("2024-02-29T01:23:45");
        assertThat(FileInterval.YEAR.getFilename(dateTime)).isEqualTo("2024");
        assertThat(FileInterval.MONTH.getFilename(dateTime)).isEqualTo("202402");
        assertThat(FileInterval.DAY.getFilename(dateTime)).isEqualTo("20240229");
    }

    @Test
    public void testGetStart() {
        assertThat(FileInterval.YEAR.getStart("2024")).isEqualTo(LocalDateTime.parse("2024-01-01T00:00:00"));
        assertThat(FileInterval.MONTH.getStart("202402")).isEqualTo(LocalDateTime.parse("2024-02-01T00:00:00"));
        assertThat(FileInterval.DAY.getStart("20240229")).isEqualTo(LocalDateTime.parse("2024-02-29T00:00:00"));
    }

    @Test
    public void testGetRange() {
        LocalDateTime dateTime = LocalDateTime.parse("2024-01-01T00:00:00");
        assertThat(FileInterval.YEAR.getRange(dateTime)).isEqualTo(Range.closed(dateTime, LocalDateTime.parse("2025-01-01T00:00:00").minusNanos(1)));
        assertThat(FileInterval.MONTH.getRange(dateTime)).isEqualTo(Range.closed(dateTime, LocalDateTime.parse("2024-02-01T00:00:00").minusNanos(1)));
        assertThat(FileInterval.DAY.getRange(dateTime)).isEqualTo(Range.closed(dateTime, LocalDateTime.parse("2024-01-02T00:00:00").minusNanos(1)));
    }

    @Test
    public void testGetDuration() {
        LocalDateTime dateTime = LocalDateTime.parse("2024-01-01T00:00:00");
        assertThat(FileInterval.YEAR.getDuration(dateTime)).isEqualTo(Duration.between(dateTime, LocalDateTime.parse("2025-01-01T00:00:00")));
        assertThat(FileInterval.MONTH.getDuration(dateTime)).isEqualTo(Duration.between(dateTime, LocalDateTime.parse("2024-02-01T00:00:00")));
        assertThat(FileInterval.DAY.getDuration(dateTime)).isEqualTo(Duration.between(dateTime, LocalDateTime.parse("2024-01-02T00:00:00")));
    }

    @Test
    public void testCalculateSize() {
        LocalDateTime dateTime = LocalDateTime.parse("2024-01-01T00:00:00");
        assertThat(FileInterval.YEAR.calculateSize(dateTime, Duration.of(1, ChronoUnit.SECONDS))).isEqualTo(31622400);
        assertThat(FileInterval.YEAR.calculateSize(dateTime, Duration.of(1, ChronoUnit.MINUTES))).isEqualTo(527040);
        assertThat(FileInterval.YEAR.calculateSize(dateTime, Duration.of(1, ChronoUnit.HOURS))).isEqualTo(8784);
        assertThat(FileInterval.MONTH.calculateSize(dateTime, Duration.of(1, ChronoUnit.SECONDS))).isEqualTo(2678400);
        assertThat(FileInterval.MONTH.calculateSize(dateTime, Duration.of(1, ChronoUnit.MINUTES))).isEqualTo(44640);
        assertThat(FileInterval.MONTH.calculateSize(dateTime, Duration.of(1, ChronoUnit.HOURS))).isEqualTo(744);
        assertThat(FileInterval.DAY.calculateSize(dateTime, Duration.of(1, ChronoUnit.SECONDS))).isEqualTo(86400);
        assertThat(FileInterval.DAY.calculateSize(dateTime, Duration.of(1, ChronoUnit.MINUTES))).isEqualTo(1440);
        assertThat(FileInterval.DAY.calculateSize(dateTime, Duration.of(1, ChronoUnit.HOURS))).isEqualTo(24);
    }

}
