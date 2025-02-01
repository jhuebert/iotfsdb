package org.huebert.iotfsdb.service;


import com.google.common.collect.Range;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.partition.PartitionAdapter;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.locks.ReadWriteLock;

public record PartitionRange(
    @Valid @NotNull PartitionKey key,
    @NotNull Range<LocalDateTime> range,
    @NotNull Duration interval,
    @NotNull PartitionAdapter adapter,
    @NotNull ReadWriteLock rwLock
) {

    public int getIndex(LocalDateTime dateTime) {
        return (int) Duration.between(range.lowerEndpoint(), dateTime).dividedBy(interval);
    }

    public long getSize() {
        return getIndex(range.upperEndpoint()) + 1;
    }

    public void withRead(LockUtil.RunnableWithException runnable) {
        LockUtil.withRead(rwLock, runnable);
    }

    public void withWrite(LockUtil.RunnableWithException runnable) {
        LockUtil.withWrite(rwLock, runnable);
    }

}
