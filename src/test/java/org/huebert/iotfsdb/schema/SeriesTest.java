package org.huebert.iotfsdb.schema;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SeriesTest {

    @Test
    public void testCalculateSize() {
        Series series = new Series("a", SeriesType.FLOAT, 60, FileInterval.YEAR);
        assertThat(series.calculateSize(LocalDateTime.parse("2024-10-03T01:23:45"))).isEqualTo(525600);
    }

    @Test
    public void testCheckValid() {
        Series.checkValid(new Series("a", SeriesType.FLOAT, 1, FileInterval.DAY));
        assertThrows(IllegalArgumentException.class, () -> Series.checkValid(null));
        assertThrows(IllegalArgumentException.class, () -> Series.checkValid(new Series(null, SeriesType.FLOAT, 1, FileInterval.DAY)));
        assertThrows(IllegalArgumentException.class, () -> Series.checkValid(new Series("a", null, 1, FileInterval.DAY)));
        assertThrows(IllegalArgumentException.class, () -> Series.checkValid(new Series("a", SeriesType.FLOAT, 0, FileInterval.DAY)));
        assertThrows(IllegalArgumentException.class, () -> Series.checkValid(new Series("a", SeriesType.FLOAT, 86401, FileInterval.DAY)));
        assertThrows(IllegalArgumentException.class, () -> Series.checkValid(new Series("a", SeriesType.FLOAT, 1, null)));
    }

}
