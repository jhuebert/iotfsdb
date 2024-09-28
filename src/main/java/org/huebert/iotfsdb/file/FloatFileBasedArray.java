package org.huebert.iotfsdb.file;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.FloatBuffer;
import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class FloatFileBasedArray implements FileBasedArray<Float> {

    private final int size;

    private final boolean readOnly;

    private final RandomAccessFile randomAccessFile;

    private final FloatBuffer floatBuffer;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static FloatFileBasedArray read(File file, boolean readOnly) {
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
            FloatBuffer floatBuffer = randomAccessFile.getChannel()
                .map(combinedReadOnly ? READ_ONLY : READ_WRITE, 0, numBytes)
                .asFloatBuffer();
            return new FloatFileBasedArray(size, combinedReadOnly, randomAccessFile, floatBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static FloatFileBasedArray create(File file, int size) {
        Preconditions.checkNotNull(file);
        Preconditions.checkArgument(!file.exists());

        Preconditions.checkArgument(size > 0);
        int numBytes = size * 4;

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            FloatBuffer floatBuffer = randomAccessFile.getChannel()
                .map(READ_WRITE, 0, numBytes)
                .asFloatBuffer();
            for (int i = 0; i < size; i++) {
                floatBuffer.put(Float.NaN);
            }
            floatBuffer.rewind();
            return new FloatFileBasedArray(size, false, randomAccessFile, floatBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public FloatFileBasedArray(int size, boolean readOnly, RandomAccessFile randomAccessFile, FloatBuffer floatBuffer) {
        this.size = size;
        this.readOnly = readOnly;
        this.randomAccessFile = randomAccessFile;
        this.floatBuffer = floatBuffer;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public Float get(int index) {
        Preconditions.checkElementIndex(index, size);
        rwLock.readLock().lock();
        float result;
        try {
            result = floatBuffer.get(index);
        } finally {
            rwLock.readLock().unlock();
        }
        return Float.isNaN(result) ? null : result;
    }

    @Override
    public List<Float> get(int start, int end) {
        Preconditions.checkPositionIndexes(start, end, size);
        int length = end - start;
        float[] result = new float[length];
        rwLock.readLock().lock();
        try {
            floatBuffer.get(start, result);
        } finally {
            rwLock.readLock().unlock();
        }
        return new NullableArray(result);
    }

    @Override
    public void set(int index, Float value) {
        Preconditions.checkArgument(!readOnly);
        Preconditions.checkElementIndex(index, size);
        rwLock.writeLock().lock();
        try {
            floatBuffer.put(index, value != null ? value : Float.NaN);
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

    public static class NullableArray extends AbstractList<Float> {

        private final float[] values;

        private NullableArray(float[] values) {
            this.values = values;
        }

        @Override
        public Float get(int index) {
            Preconditions.checkElementIndex(index, values.length);
            float result = values[index];
            return Float.isNaN(result) ? null : result;
        }

        @Override
        public int size() {
            return values.length;
        }

    }

}
