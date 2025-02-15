package org.huebert.iotfsdb.service;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LockUtilTest {

    @Test
    public void testWithLock() {
        ReentrantLock lock = new ReentrantLock();
        AtomicInteger value = new AtomicInteger(0);
        LockUtil.withLock(lock, () -> {
            assertThat(lock.isLocked()).isTrue();
            value.incrementAndGet();
        });
        assertThat(lock.isLocked()).isFalse();
        assertThat(value.get()).isEqualTo(1);
    }

    @Test
    public void testWithLockException() {
        ReentrantLock lock = new ReentrantLock();
        assertThrows(RuntimeException.class, () -> LockUtil.withLock(lock, () -> {
            assertThat(lock.isLocked()).isTrue();
            throw new IllegalArgumentException();
        }));
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    public void testWithRead() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        AtomicInteger value = new AtomicInteger(0);
        LockUtil.withRead(lock, () -> {
            assertThat(lock.getReadLockCount()).isEqualTo(1);
            value.incrementAndGet();
        });
        assertThat(lock.getReadLockCount()).isEqualTo(0);
        assertThat(value.get()).isEqualTo(1);
    }

    @Test
    public void testWithWrite() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        AtomicInteger value = new AtomicInteger(0);
        LockUtil.withWrite(lock, () -> {
            assertThat(lock.getWriteHoldCount()).isEqualTo(1);
            value.incrementAndGet();
        });
        assertThat(lock.getWriteHoldCount()).isEqualTo(0);
        assertThat(value.get()).isEqualTo(1);
    }

}
