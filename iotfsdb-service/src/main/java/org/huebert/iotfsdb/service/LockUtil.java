package org.huebert.iotfsdb.service;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class LockUtil {

    public static void withRead(ReadWriteLock rwLock, RunnableWithException runnable) {
        withLock(rwLock.readLock(), runnable);
    }

    public static void withWrite(ReadWriteLock rwLock, RunnableWithException runnable) {
        withLock(rwLock.writeLock(), runnable);
    }

    public static void withLock(Lock lock, RunnableWithException runnable) {
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
