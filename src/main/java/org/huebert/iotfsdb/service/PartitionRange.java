package org.huebert.iotfsdb.service;


import com.google.common.collect.Range;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.partition.PartitionAdapter;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.locks.Lock;
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
        return Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(interval) + 1;
    }

    public void withRead(RunnableWithException runnable) {
        withLock(rwLock.readLock(), runnable);
    }

    public void withWrite(RunnableWithException runnable) {
        withLock(rwLock.writeLock(), runnable);
    }

    private static void withLock(Lock lock, RunnableWithException runnable) {
        lock.lock();
        try {
            runnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public interface RunnableWithException {
        void run() throws Exception;
    }

}
