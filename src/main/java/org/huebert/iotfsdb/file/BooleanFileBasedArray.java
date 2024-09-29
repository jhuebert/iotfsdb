package org.huebert.iotfsdb.file;

import com.google.common.base.Preconditions;

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

    private final int size;

    private final boolean readOnly;

    private final RandomAccessFile randomAccessFile;

    private final ByteBuffer byteBuffer;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static BooleanFileBasedArray read(File file, boolean readOnly) {
        Preconditions.checkNotNull(file);
        Preconditions.checkArgument(file.exists());
        Preconditions.checkArgument(file.isFile());
        Preconditions.checkArgument(file.canRead());

        long numBytes = file.length();
        Preconditions.checkArgument(numBytes > 0);
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
        Preconditions.checkNotNull(file);
        Preconditions.checkArgument(!file.exists());
        Preconditions.checkArgument(size > 0);
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            ByteBuffer byteBuffer = randomAccessFile.getChannel()
                .map(READ_WRITE, 0, size);
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
        Preconditions.checkPositionIndexes(start, end, size - 1);
        int length = end - start + 1;
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
        Preconditions.checkArgument(!readOnly);
        Preconditions.checkElementIndex(index, size);
        byte byteValue = Byte.MIN_VALUE;
        if (value != null) {
            byteValue = (byte) (value ? 1 : 0);
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
            Preconditions.checkElementIndex(index, values.length);
            byte result = values[index];
            return result == Byte.MIN_VALUE ? null : result != 0;
        }

        @Override
        public int size() {
            return values.length;
        }

    }

}
