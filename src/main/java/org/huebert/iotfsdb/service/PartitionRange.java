package org.huebert.iotfsdb.service;


import com.google.common.collect.Range;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public class PartitionRange {

    @Getter
    @Valid
    @NotNull
    private final PartitionKey key;

    @Getter
    @NotNull
    private final Range<LocalDateTime> range;

    @Getter
    @NotNull
    private final Duration interval;

    @Getter
    @NotNull
    private final PartitionAdapter adapter;

    private final long intervalMillis;

    private final LocalDateTime lowerEndpoint;

    @Getter
    private final long size;

    public PartitionRange(PartitionKey key, Range<LocalDateTime> range, Duration interval, PartitionAdapter adapter) {
        this.key = key;
        this.range = range;
        this.interval = interval;
        this.adapter = adapter;

        intervalMillis = interval.toMillis();
        lowerEndpoint = range.lowerEndpoint();

        size = getIndex(range.upperEndpoint()) + 1;
    }

    public List<Number> getStream(PartitionByteBuffer buffer) {
        return buffer.withRead(b -> adapter.getStream(b, 0, (int) size).toList());
    }

    public List<Number> getStream(PartitionByteBuffer buffer, Range<LocalDateTime> current) {
        Range<LocalDateTime> intersection = range.intersection(current);
        int fromIndex = getIndex(intersection.lowerEndpoint());
        int toIndex = getIndex(intersection.upperEndpoint());
        return buffer.withRead(b -> adapter.getStream(b, fromIndex, toIndex - fromIndex + 1).toList());
    }

    public int getIndex(LocalDateTime dateTime) {
        return (int) (Duration.between(lowerEndpoint, dateTime).toMillis() / intervalMillis);
    }

}
