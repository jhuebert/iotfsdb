package org.huebert.iotfsdb.service;

import com.google.common.collect.Range;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.schema.FindDataRequest;
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

        Duration rangeDuration = Duration.between(request.getFrom(), request.getTo());

        boolean hasSize = request.getSize() != null;
        boolean hasInterval = request.getInterval() != null;

        Duration duration;
        int count = properties.getMaxQuerySize();
        if (hasInterval) {
            if (hasSize) {
                count = Math.min(count, request.getSize());
            }
            duration = Duration.ofMillis(request.getInterval());
            int intervalCount = (int) rangeDuration.dividedBy(duration);
            if (intervalCount < count) {
                count = intervalCount;
            } else {
                duration = rangeDuration.dividedBy(count);
            }
        } else {
            if (hasSize) {
                count = Math.min(count, request.getSize());
            }
            duration = rangeDuration.dividedBy(count);
        }

        List<Range<ZonedDateTime>> ranges = new ArrayList<>(count);
        ZonedDateTime start = request.getFrom();
        for (int i = 0; i < count; i++) {
            ZonedDateTime end = start.plus(duration);
            ranges.add(Range.closed(start, end.minusNanos(1)));
            start = end;
        }
        return ranges;
    }

}
