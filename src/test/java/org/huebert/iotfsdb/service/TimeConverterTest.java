package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeConverterTest {

    @Test
    public void testToUtc() {
        ZonedDateTime test = ZonedDateTime.parse(("2024-11-11T21:30:28-06:00"));
        LocalDateTime expected = LocalDateTime.parse(("2024-11-12T03:30:28"));
        assertThat(TimeConverter.toUtc(test)).isEqualTo(expected);
    }

    @Test
    public void testToUtc_Range() {
        ZonedDateTime test1 = ZonedDateTime.parse(("2024-11-11T21:30:28-06:00"));
        ZonedDateTime test2 = ZonedDateTime.parse(("2024-11-11T22:30:28-06:00"));
        LocalDateTime expected1 = LocalDateTime.parse(("2024-11-12T03:30:28"));
        LocalDateTime expected2 = LocalDateTime.parse(("2024-11-12T04:30:28"));
        assertThat(TimeConverter.toUtc(Range.closed(test1, test2))).isEqualTo(Range.closed(expected1, expected2));
    }

}
