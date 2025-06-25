package org.huebert.iotfsdb.service;


import com.google.common.collect.Range;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.huebert.iotfsdb.partition.PartitionAdapter;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Stream;

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

    @Getter
    @NotNull
    private final ReadWriteLock rwLock;

    private final long intervalMillis;

    private final LocalDateTime lowerEndpoint;

    @Getter
    private final long size;

    public PartitionRange(PartitionKey key, Range<LocalDateTime> range, Duration interval, PartitionAdapter adapter, ReadWriteLock rwLock) {
        this.key = key;
        this.range = range;
        this.interval = interval;
        this.adapter = adapter;
        this.rwLock = rwLock;

        intervalMillis = interval.toMillis();
        lowerEndpoint = range.lowerEndpoint();

        size = getIndex(range.upperEndpoint()) + 1;
    }

    public Stream<Number> getStream(ByteBuffer buffer) {
        return adapter.getStream(buffer, 0, (int) size);
    }

    public Stream<Number> getStream(ByteBuffer buffer, Range<LocalDateTime> current) {
        Range<LocalDateTime> intersection = range.intersection(current);
        int fromIndex = getIndex(intersection.lowerEndpoint());
        int toIndex = getIndex(intersection.upperEndpoint());
        return adapter.getStream(buffer, fromIndex, toIndex - fromIndex + 1);
    }

    public int getIndex(LocalDateTime dateTime) {
        return (int) (Duration.between(lowerEndpoint, dateTime).toMillis() / intervalMillis);
    }

    public void withRead(LockUtil.RunnableWithException runnable) {
        LockUtil.withRead(rwLock, runnable);
    }

    public void withWrite(LockUtil.RunnableWithException runnable) {
        LockUtil.withWrite(rwLock, runnable);
    }

}
