package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Validated
@Service
public class IntervalService {

    private final IotfsdbProperties properties;

    public IntervalService(@NotNull IotfsdbProperties properties) {
        this.properties = properties;
    }

    public List<Range<ZonedDateTime>> getIntervalRanges(@Valid @NotNull FindDataRequest request) {
        Range<ZonedDateTime> dateTimeRange = request.getRange();
        Duration rangeDuration = Duration.between(dateTimeRange.lowerEndpoint(), dateTimeRange.upperEndpoint());

        int count = properties.getQuery().getMaxSize();
        if (request.getSize() != null) {
            count = Math.min(count, request.getSize());
        }

        Duration duration;
        if (request.getInterval() != null) {
            duration = Duration.ofMillis(request.getInterval());
            int intervalCount = (int) rangeDuration.dividedBy(duration);
            if (intervalCount < count) {
                count = intervalCount;
            } else {
                duration = rangeDuration.dividedBy(count);
            }
        } else {
            duration = rangeDuration.dividedBy(count);
        }

        return createRanges(dateTimeRange.lowerEndpoint(), duration, count);
    }

    private List<Range<ZonedDateTime>> createRanges(ZonedDateTime start, Duration duration, int count) {
        List<Range<ZonedDateTime>> ranges = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ZonedDateTime end = start.plus(duration);
            ranges.add(Range.closed(start, end.minusNanos(1)));
            start = end;
        }
        return ranges;
    }

}
