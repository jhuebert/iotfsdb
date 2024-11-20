package org.huebert.iotfsdb.schema;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.Period;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionPeriodTest {

    @Test
    public void testPeriod() {
        assertThat(PartitionPeriod.YEAR.getPeriod()).isEqualTo(Period.ofYears(1));
        assertThat(PartitionPeriod.MONTH.getPeriod()).isEqualTo(Period.ofMonths(1));
        assertThat(PartitionPeriod.DAY.getPeriod()).isEqualTo(Period.ofDays(1));
    }

    @Test
    public void testFindMatch() {
        assertThat(PartitionPeriod.YEAR.matches("202")).isEqualTo(false);
        assertThat(PartitionPeriod.YEAR.matches("2024")).isEqualTo(true);
        assertThat(PartitionPeriod.YEAR.matches("202410")).isEqualTo(false);
        assertThat(PartitionPeriod.YEAR.matches("20241003")).isEqualTo(false);
        assertThat(PartitionPeriod.YEAR.matches("2024100301")).isEqualTo(false);

        assertThat(PartitionPeriod.MONTH.matches("202")).isEqualTo(false);
        assertThat(PartitionPeriod.MONTH.matches("2024")).isEqualTo(false);
        assertThat(PartitionPeriod.MONTH.matches("202410")).isEqualTo(true);
        assertThat(PartitionPeriod.MONTH.matches("20241003")).isEqualTo(false);
        assertThat(PartitionPeriod.MONTH.matches("2024100301")).isEqualTo(false);

        assertThat(PartitionPeriod.DAY.matches("202")).isEqualTo(false);
        assertThat(PartitionPeriod.DAY.matches("2024")).isEqualTo(false);
        assertThat(PartitionPeriod.DAY.matches("202410")).isEqualTo(false);
        assertThat(PartitionPeriod.DAY.matches("20241003")).isEqualTo(true);
        assertThat(PartitionPeriod.DAY.matches("2024100301")).isEqualTo(false);
    }

    @Test
    public void testGetStart() {
        LocalDateTime dateTime = LocalDateTime.parse("2024-02-29T01:23:45");
        assertThat(PartitionPeriod.YEAR.getStart(dateTime)).isEqualTo(LocalDateTime.parse("2024-01-01T00:00:00"));
        assertThat(PartitionPeriod.MONTH.getStart(dateTime)).isEqualTo(LocalDateTime.parse("2024-02-01T00:00:00"));
        assertThat(PartitionPeriod.DAY.getStart(dateTime)).isEqualTo(LocalDateTime.parse("2024-02-29T00:00:00"));
    }

    @Test
    public void testGetFilename() {
        LocalDateTime dateTime = LocalDateTime.parse("2024-02-29T01:23:45");
        assertThat(PartitionPeriod.YEAR.getFilename(dateTime)).isEqualTo("2024");
        assertThat(PartitionPeriod.MONTH.getFilename(dateTime)).isEqualTo("202402");
        assertThat(PartitionPeriod.DAY.getFilename(dateTime)).isEqualTo("20240229");
    }

    @Test
    public void testParseStart() {
        assertThat(PartitionPeriod.YEAR.parseStart("2024")).isEqualTo(LocalDateTime.parse("2024-01-01T00:00:00"));
        assertThat(PartitionPeriod.MONTH.parseStart("202402")).isEqualTo(LocalDateTime.parse("2024-02-01T00:00:00"));
        assertThat(PartitionPeriod.DAY.parseStart("20240229")).isEqualTo(LocalDateTime.parse("2024-02-29T00:00:00"));
    }

}
