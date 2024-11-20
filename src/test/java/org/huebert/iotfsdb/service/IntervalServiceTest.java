package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.schema.FindDataRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class IntervalServiceTest {

    @Mock
    private IotfsdbProperties properties;

    @InjectMocks
    private IntervalService intervalService;

    @Test
    public void testGetIntervalRanges_Size() {
        FindDataRequest request = new FindDataRequest();
        request.setSize(2);
        request.setFrom(ZonedDateTime.parse(("2024-11-11T00:00:00-06:00")));
        request.setTo(ZonedDateTime.parse(("2024-11-11T04:00:00-06:00")));

        when(properties.getMaxQuerySize()).thenReturn(10);

        List<Range<ZonedDateTime>> ranges = intervalService.getIntervalRanges(request);
        assertThat(ranges.size()).isEqualTo(2);
        assertThat(ranges.get(0)).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T00:00:00-06:00"), ZonedDateTime.parse("2024-11-11T02:00:00-06:00").minusNanos(1)));
        assertThat(ranges.get(1)).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T02:00:00-06:00"), ZonedDateTime.parse("2024-11-11T04:00:00-06:00").minusNanos(1)));
    }

    @Test
    public void testGetIntervalRanges_Size_Limited() {
        FindDataRequest request = new FindDataRequest();
        request.setSize(2);
        request.setFrom(ZonedDateTime.parse(("2024-11-11T00:00:00-06:00")));
        request.setTo(ZonedDateTime.parse(("2024-11-11T04:00:00-06:00")));

        when(properties.getMaxQuerySize()).thenReturn(1);

        List<Range<ZonedDateTime>> ranges = intervalService.getIntervalRanges(request);
        assertThat(ranges.size()).isEqualTo(1);
        assertThat(ranges.getFirst()).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T00:00:00-06:00"), ZonedDateTime.parse("2024-11-11T04:00:00-06:00").minusNanos(1)));
    }

    @Test
    public void testGetIntervalRanges_Interval() {
        FindDataRequest request = new FindDataRequest();
        request.setInterval(7200000L);
        request.setFrom(ZonedDateTime.parse(("2024-11-11T00:00:00-06:00")));
        request.setTo(ZonedDateTime.parse(("2024-11-11T04:00:00-06:00")));

        when(properties.getMaxQuerySize()).thenReturn(10);

        List<Range<ZonedDateTime>> ranges = intervalService.getIntervalRanges(request);
        assertThat(ranges.size()).isEqualTo(2);
        assertThat(ranges.get(0)).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T00:00:00-06:00"), ZonedDateTime.parse("2024-11-11T02:00:00-06:00").minusNanos(1)));
        assertThat(ranges.get(1)).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T02:00:00-06:00"), ZonedDateTime.parse("2024-11-11T04:00:00-06:00").minusNanos(1)));
    }

    @Test
    public void testGetIntervalRanges_Interval_Limited() {
        FindDataRequest request = new FindDataRequest();
        request.setInterval(7200000L);
        request.setFrom(ZonedDateTime.parse(("2024-11-11T00:00:00-06:00")));
        request.setTo(ZonedDateTime.parse(("2024-11-11T04:00:00-06:00")));

        when(properties.getMaxQuerySize()).thenReturn(1);

        List<Range<ZonedDateTime>> ranges = intervalService.getIntervalRanges(request);
        assertThat(ranges.size()).isEqualTo(1);
        assertThat(ranges.getFirst()).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T00:00:00-06:00"), ZonedDateTime.parse("2024-11-11T04:00:00-06:00").minusNanos(1)));
    }

    @Test
    public void testGetIntervalRanges_None() {
        FindDataRequest request = new FindDataRequest();
        request.setFrom(ZonedDateTime.parse(("2024-11-11T00:00:00-06:00")));
        request.setTo(ZonedDateTime.parse(("2024-11-11T04:00:00-06:00")));

        when(properties.getMaxQuerySize()).thenReturn(10);

        List<Range<ZonedDateTime>> ranges = intervalService.getIntervalRanges(request);
        assertThat(ranges.size()).isEqualTo(10);
        assertThat(ranges.get(0)).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T00:00:00-06:00"), ZonedDateTime.parse("2024-11-11T00:24:00-06:00").minusNanos(1)));
        assertThat(ranges.get(9)).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T03:36:00-06:00"), ZonedDateTime.parse("2024-11-11T04:00:00-06:00").minusNanos(1)));
    }

    @Test
    public void testGetIntervalRanges_Interval_Both() {
        FindDataRequest request = new FindDataRequest();
        request.setSize(2);
        request.setInterval(3600000L);
        request.setFrom(ZonedDateTime.parse(("2024-11-11T00:00:00-06:00")));
        request.setTo(ZonedDateTime.parse(("2024-11-11T04:00:00-06:00")));

        when(properties.getMaxQuerySize()).thenReturn(10);

        List<Range<ZonedDateTime>> ranges = intervalService.getIntervalRanges(request);
        assertThat(ranges.size()).isEqualTo(2);
        assertThat(ranges.get(0)).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T00:00:00-06:00"), ZonedDateTime.parse("2024-11-11T02:00:00-06:00").minusNanos(1)));
        assertThat(ranges.get(1)).isEqualTo(Range.closed(ZonedDateTime.parse("2024-11-11T02:00:00-06:00"), ZonedDateTime.parse("2024-11-11T04:00:00-06:00").minusNanos(1)));
    }

}
