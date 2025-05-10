package org.huebert.iotfsdb.service;


import com.google.common.collect.Range;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.huebert.iotfsdb.partition.PartitionAdapter;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.locks.ReadWriteLock;

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

    public int getIndex(LocalDateTime dateTime) {
        long between = Duration.between(lowerEndpoint, dateTime).toMillis();
        return (int) (between / intervalMillis);
    }

    public void withRead(LockUtil.RunnableWithException runnable) {
        LockUtil.withRead(rwLock, runnable);
    }

    public void withWrite(LockUtil.RunnableWithException runnable) {
        LockUtil.withWrite(rwLock, runnable);
    }

}
