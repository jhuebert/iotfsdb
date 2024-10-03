package org.huebert.iotfsdb.file;

import org.huebert.iotfsdb.Util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class BooleanFileBasedArray implements FileBasedArray<Boolean> {

    private static final byte NULL_VALUE = Byte.MIN_VALUE;

    private static final byte FALSE_VALUE = (byte) 0;

    private static final byte TRUE_VALUE = (byte) 1;

    private final int size;

    private final boolean readOnly;

    private final RandomAccessFile randomAccessFile;

    private final ByteBuffer byteBuffer;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static BooleanFileBasedArray read(File file, boolean readOnly) {
        Util.checkFile(file);

        long numBytes = file.length();
        if (numBytes == 0) {
            throw new IllegalArgumentException(String.format("file (%s) is empty", file));
        }

        int size = (int) numBytes;

        boolean combinedReadOnly = readOnly || !file.canWrite();

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, combinedReadOnly ? "r" : "rw");
            ByteBuffer byteBuffer = randomAccessFile.getChannel()
                .map(combinedReadOnly ? READ_ONLY : READ_WRITE, 0, numBytes);
            return new BooleanFileBasedArray(size, combinedReadOnly, randomAccessFile, byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static BooleanFileBasedArray create(File file, int size) {

        if (file.exists()) {
            throw new IllegalArgumentException(String.format("file (%s) already exists", file));
        }

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            ByteBuffer byteBuffer = randomAccessFile.getChannel().map(READ_WRITE, 0, size);
            for (int i = 0; i < size; i++) {
                byteBuffer.put(Byte.MIN_VALUE);
            }
            byteBuffer.rewind();
            return new BooleanFileBasedArray(size, false, randomAccessFile, byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BooleanFileBasedArray(int size, boolean readOnly, RandomAccessFile randomAccessFile, ByteBuffer byteBuffer) {
        this.size = size;
        this.readOnly = readOnly;
        this.randomAccessFile = randomAccessFile;
        this.byteBuffer = byteBuffer;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<Boolean> get(int start, int end) {

        int length = end - start;
        byte[] result = new byte[length];

        rwLock.readLock().lock();
        try {
            byteBuffer.get(start, result);
        } finally {
            rwLock.readLock().unlock();
        }

        return new NullableArray(result);
    }

    @Override
    public void set(int index, Boolean value) {

        if (readOnly) {
            throw new IllegalStateException("file is read only");
        }

        byte byteValue;
        if (value == null) {
            byteValue = NULL_VALUE;
        } else {
            byteValue = value ? TRUE_VALUE : FALSE_VALUE;
        }

        rwLock.writeLock().lock();
        try {
            byteBuffer.put(index, byteValue);
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

    public static class NullableArray extends AbstractList<Boolean> {

        private final byte[] values;

        private NullableArray(byte[] values) {
            this.values = values;
        }

        @Override
        public Boolean get(int index) {
            byte result = values[index];
            if (result == NULL_VALUE) {
                return null;
            }
            return result != FALSE_VALUE;
        }

        @Override
        public int size() {
            return values.length;
        }

    }

}
