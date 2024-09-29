package org.huebert.iotfsdb.file;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class IntegerFileBasedArray implements FileBasedArray<Integer> {

    private final int size;

    private final boolean readOnly;

    private final RandomAccessFile randomAccessFile;

    private final IntBuffer intBuffer;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static IntegerFileBasedArray read(File file, boolean readOnly) {
        Preconditions.checkNotNull(file);
        Preconditions.checkArgument(file.exists());
        Preconditions.checkArgument(file.isFile());
        Preconditions.checkArgument(file.canRead());

        long numBytes = file.length();
        Preconditions.checkArgument(numBytes > 0);
        Preconditions.checkArgument(numBytes % 4 == 0);
        int size = (int) (numBytes / 4);

        boolean combinedReadOnly = readOnly || !file.canWrite();

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, combinedReadOnly ? "r" : "rw");
            IntBuffer intBuffer = randomAccessFile.getChannel()
                .map(combinedReadOnly ? READ_ONLY : READ_WRITE, 0, numBytes)
                .asIntBuffer();
            return new IntegerFileBasedArray(size, combinedReadOnly, randomAccessFile, intBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static IntegerFileBasedArray create(File file, int size) {
        Preconditions.checkNotNull(file);
        Preconditions.checkArgument(!file.exists());

        Preconditions.checkArgument(size > 0);
        int numBytes = size * 4;

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            IntBuffer intBuffer = randomAccessFile.getChannel()
                .map(READ_WRITE, 0, numBytes)
                .asIntBuffer();
            for (int i = 0; i < size; i++) {
                intBuffer.put(Integer.MIN_VALUE);
            }
            intBuffer.rewind();
            return new IntegerFileBasedArray(size, false, randomAccessFile, intBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private IntegerFileBasedArray(int size, boolean readOnly, RandomAccessFile randomAccessFile, IntBuffer intBuffer) {
        this.size = size;
        this.readOnly = readOnly;
        this.randomAccessFile = randomAccessFile;
        this.intBuffer = intBuffer;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<Integer> get(int start, int end) {
        Preconditions.checkPositionIndexes(start, end, size - 1);
        int length = end - start + 1;
        int[] result = new int[length];
        rwLock.readLock().lock();
        try {
            intBuffer.get(start, result);
        } finally {
            rwLock.readLock().unlock();
        }
        return new NullableArray(result);
    }

    @Override
    public void set(int index, Integer value) {
        Preconditions.checkArgument(!readOnly);
        Preconditions.checkElementIndex(index, size);
        rwLock.writeLock().lock();
        try {
            intBuffer.put(index, value != null ? value : Integer.MIN_VALUE);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws Exception {
        rwLock.writeLock().lock();
        try {
            randomAccessFile.close();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public static class NullableArray extends AbstractList<Integer> {

        private final int[] values;

        private NullableArray(int[] values) {
            this.values = values;
        }

        @Override
        public Integer get(int index) {
            Preconditions.checkElementIndex(index, values.length);
            int result = values[index];
            return result == Integer.MIN_VALUE ? null : result;
        }

        @Override
        public int size() {
            return values.length;
        }

    }

}
