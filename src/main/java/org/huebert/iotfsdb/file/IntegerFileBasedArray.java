package org.huebert.iotfsdb.file;

import com.google.common.base.Preconditions;
import org.huebert.iotfsdb.Util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class IntegerFileBasedArray implements FileBasedArray<Integer> {

    private static final int NULL_VALUE = Integer.MIN_VALUE;

    private final int size;

    private final boolean readOnly;

    private final RandomAccessFile randomAccessFile;

    private final MappedByteBuffer mappedByteBuffer;

    private final IntBuffer intBuffer;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static IntegerFileBasedArray read(File file, boolean readOnly) {
        Util.checkFile(file);

        long numBytes = file.length();
        if (numBytes == 0) {
            throw new IllegalArgumentException(String.format("file (%s) is empty", file));
        }

        if (numBytes % 4 != 0) {
            throw new IllegalArgumentException(String.format("file (%s) size (%d) is not a multiple of four", file, numBytes));
        }

        int size = (int) (numBytes / 4);

        boolean combinedReadOnly = readOnly || !file.canWrite();

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, combinedReadOnly ? "r" : "rw");
            MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel()
                .map(combinedReadOnly ? READ_ONLY : READ_WRITE, 0, numBytes);
            IntBuffer intBuffer = mappedByteBuffer.asIntBuffer();
            return new IntegerFileBasedArray(size, combinedReadOnly, randomAccessFile, mappedByteBuffer, intBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static IntegerFileBasedArray create(File file, int size) {

        if (file.exists()) {
            throw new IllegalArgumentException(String.format("file (%s) already exists", file));
        }

        int numBytes = size * 4;

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel()
                .map(READ_WRITE, 0, numBytes);
            IntBuffer intBuffer = mappedByteBuffer.asIntBuffer();
            for (int i = 0; i < size; i++) {
                intBuffer.put(Integer.MIN_VALUE);
            }
            intBuffer.rewind();
            mappedByteBuffer.force();
            return new IntegerFileBasedArray(size, false, randomAccessFile, mappedByteBuffer, intBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private IntegerFileBasedArray(int size, boolean readOnly, RandomAccessFile randomAccessFile, MappedByteBuffer mappedByteBuffer, IntBuffer intBuffer) {
        this.size = size;
        this.readOnly = readOnly;
        this.randomAccessFile = randomAccessFile;
        this.mappedByteBuffer = mappedByteBuffer;
        this.intBuffer = intBuffer;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<Integer> get(int start, int length) {

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

        if (readOnly) {
            throw new IllegalStateException("file is read only");
        }

        int intValue = value == null ? NULL_VALUE : value;

        rwLock.writeLock().lock();
        try {
            intBuffer.put(index, intValue);
            mappedByteBuffer.force(index * 4, 4);
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
            return result == NULL_VALUE ? null : result;
        }

        @Override
        public int size() {
            return values.length;
        }

    }

}
