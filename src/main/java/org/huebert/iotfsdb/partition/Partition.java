package org.huebert.iotfsdb.partition;

import com.google.common.base.Function;
import com.google.common.collect.Range;
import lombok.Getter;
import org.huebert.iotfsdb.util.TriFunction;
import org.huebert.iotfsdb.util.Util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;
import java.util.function.BiFunction;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * This class is not thread safe and relies on external synchronization.
 *
 * @param <T> Type of data in this partition
 */
public abstract class Partition<T extends Number> extends AbstractList<T> implements RandomAccess, AutoCloseable {

    @Getter
    private volatile boolean open;

    private final File file;

    @Getter
    private final Duration interval;

    @Getter
    private final Range<LocalDateTime> range;

    private final int size;

    @Getter
    private final boolean readOnly;

    private final int typeSize;

    private final BiFunction<ByteBuffer, Integer, T> getType;

    private final TriFunction<ByteBuffer, Integer, T, ByteBuffer> putType;

    private final Function<String, T> convertText;

    private RandomAccessFile randomAccessFile;

    private MappedByteBuffer mappedByteBuffer;

    protected Partition(
        File file,
        LocalDateTime start,
        Period period,
        Duration interval,
        int typeSize,
        BiFunction<ByteBuffer, Integer, T> getType,
        TriFunction<ByteBuffer, Integer, T, ByteBuffer> putType,
        Function<String, T> convertText
    ) {
        this.typeSize = typeSize;
        this.getType = getType;
        this.putType = putType;
        this.convertText = convertText;
        this.file = file;

        LocalDateTime end = start.plus(period);
        this.range = Range.closed(start, end.minusNanos(1));

        if (file.exists()) {
            Util.checkFile(file);

            long numBytes = file.length();
            if (numBytes == 0) {
                throw new IllegalArgumentException(String.format("file (%s) is empty", file));
            }

            if (numBytes % typeSize != 0) {
                throw new IllegalArgumentException(String.format("file (%s) size (%d) is not a multiple of four", file, numBytes));
            }

            this.readOnly = !file.canWrite();
            this.open = false;
            this.size = (int) (numBytes / typeSize);
            this.interval = Duration.between(start, end).dividedBy(this.size);

        } else {

            this.readOnly = false;
            this.open = true;
            this.size = (int) Duration.between(start, end).dividedBy(interval);
            this.interval = interval;

            try {
                int numBytes = size * typeSize;
                this.randomAccessFile = new RandomAccessFile(file, "rw");
                this.mappedByteBuffer = randomAccessFile.getChannel().map(READ_WRITE, 0, numBytes);
                for (int i = 0; i < numBytes; i += typeSize) {
                    putType.apply(mappedByteBuffer, i, null);
                }
                mappedByteBuffer.rewind();
                mappedByteBuffer.force();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }

//    protected Partition(File file, LocalDateTime start, Period period, boolean readOnly, int typeSize, BiFunction<ByteBuffer, Integer, T> getType, TriFunction<ByteBuffer, Integer, T, ByteBuffer> putType, Function<String, T> convertText) {
//        this.file = Util.checkFile(file);
//        this.typeSize = typeSize;
//        this.getType = getType;
//        this.putType = putType;
//        this.convertText = convertText;
//
//        long numBytes = file.length();
//        if (numBytes == 0) {
//            throw new IllegalArgumentException(String.format("file (%s) is empty", file));
//        }
//
//        if (numBytes % typeSize != 0) {
//            throw new IllegalArgumentException(String.format("file (%s) size (%d) is not a multiple of four", file, numBytes));
//        }
//
//        this.size = (int) (numBytes / typeSize);
//        this.readOnly = readOnly || !file.canWrite();
//        LocalDateTime end = start.plus(period);
//        this.interval = Duration.between(start, end).dividedBy(this.size);
//        this.range = Range.closed(start, end.minusNanos(1));
//        this.open = false;
//    }
//
//    protected Partition(File file, LocalDateTime start, Period period, Duration interval, int typeSize, BiFunction<ByteBuffer, Integer, T> getType, TriFunction<ByteBuffer, Integer, T, ByteBuffer> putType, Function<String, T> convertText) {
//        this.typeSize = typeSize;
//        this.getType = getType;
//        this.putType = putType;
//        this.convertText = convertText;
//
//        if (file.exists()) {
//            throw new IllegalArgumentException(String.format("file (%s) already exists", file));
//        }
//
//        this.file = file;
//        this.readOnly = false;
//        LocalDateTime end = start.plus(period);
//        this.range = Range.closed(start, end.minusNanos(1));
//        this.interval = interval;
//        this.size = (int) Duration.between(start, end).dividedBy(interval);
//        int numBytes = size * typeSize;
//
//        try {
//            this.randomAccessFile = new RandomAccessFile(file, "rw");
//            this.mappedByteBuffer = randomAccessFile.getChannel()
//                .map(READ_WRITE, 0, numBytes);
//            for (int i = 0; i < numBytes; i += typeSize) {
//                putType.apply(mappedByteBuffer, i, null);
//            }
//            mappedByteBuffer.rewind();
//            mappedByteBuffer.force();
//            this.open = true;
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public List<T> get(Range<LocalDateTime> range) {
        Range<LocalDateTime> intersection = this.range.intersection(range);
        if (intersection.isEmpty()) {
            return List.of();
        }
        int fromIndex = getIndex(intersection.lowerEndpoint());
        int toIndex = getIndex(intersection.upperEndpoint());
        return subList(fromIndex, toIndex + 1);
    }

    public T set(LocalDateTime dateTime, String element) {
        return set(dateTime, element == null ? null : convertText.apply(element));
    }

    public T set(LocalDateTime dateTime, T element) {
        int index = getIndex(dateTime);
        return set(index, element);
    }

    public T get(LocalDateTime dateTime) {
        int index = getIndex(dateTime);
        return get(index);
    }

    private int getIndex(LocalDateTime dateTime) {
        return (int) Duration.between(range.lowerEndpoint(), dateTime).dividedBy(interval);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public T get(int index) {
        open();
        int byteOffset = index * typeSize;
        return getType.apply(mappedByteBuffer, byteOffset);
    }

    @Override
    public T set(int index, T element) {

        if (readOnly) {
            throw new IllegalStateException("file is read only");
        }

        open();

        int byteOffset = index * typeSize;
        T previous = getType.apply(mappedByteBuffer, byteOffset);
        putType.apply(mappedByteBuffer, byteOffset, element);
        mappedByteBuffer.force(byteOffset, typeSize);
        return previous;
    }

    public void open() {
        if (!open) {
            synchronized (this) {
                if (!open) {
                    try {
                        randomAccessFile = new RandomAccessFile(file, readOnly ? "r" : "rw");
                        mappedByteBuffer = randomAccessFile.getChannel()
                            .map(this.readOnly ? READ_ONLY : READ_WRITE, 0, file.length());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    open = true;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (open) {
            synchronized (this) {
                if (open) {
                    randomAccessFile.close();
                    randomAccessFile = null;
                    mappedByteBuffer = null;
                    open = false;
                }
            }
        }
    }

}
