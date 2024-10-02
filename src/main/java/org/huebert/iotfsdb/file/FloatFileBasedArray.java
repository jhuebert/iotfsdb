package org.huebert.iotfsdb.file;

import org.huebert.iotfsdb.Util;

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

    private static final float NULL_VALUE = Float.NaN;

    private final int size;

    private final boolean readOnly;

    private final RandomAccessFile randomAccessFile;

    private final FloatBuffer floatBuffer;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static FloatFileBasedArray read(File file, boolean readOnly) {
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
            FloatBuffer floatBuffer = randomAccessFile.getChannel()
                .map(combinedReadOnly ? READ_ONLY : READ_WRITE, 0, numBytes)
                .asFloatBuffer();
            return new FloatFileBasedArray(size, combinedReadOnly, randomAccessFile, floatBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static FloatFileBasedArray create(File file, int size) {

        if (file.exists()) {
            throw new IllegalArgumentException(String.format("file (%s) already exists", file));
        }

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

    private FloatFileBasedArray(int size, boolean readOnly, RandomAccessFile randomAccessFile, FloatBuffer floatBuffer) {
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
    public List<Float> get(int start, int end) {

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

        if (readOnly) {
            throw new IllegalArgumentException("file is read only");
        }

        float floatValue = value == null ? NULL_VALUE : value;

        rwLock.writeLock().lock();
        try {
            floatBuffer.put(index, floatValue);
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
            float result = values[index];
            return Float.isNaN(result) ? null : result;
        }

        @Override
        public int size() {
            return values.length;
        }

    }

}
