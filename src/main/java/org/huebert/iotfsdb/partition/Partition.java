package org.huebert.iotfsdb.partition;

import com.google.common.base.Function;
import com.google.common.collect.Range;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public abstract class Partition<T extends Number> extends AbstractList<T> implements RandomAccess, AutoCloseable {

    // Partition can be considered idle if it hasn't been updated in the last 5 minutes
    public static final int IDLE_TIME_MS = 300000;

    // Synchronize no more often than once per second
    private static final int SYNC_TIME_MS = 1000;

    private final File file;

    private final int size;

    private final int typeSize;

    private final int bitShift;

    private final BiFunction<ByteBuffer, Integer, T> getType;

    private final TriFunction<ByteBuffer, Integer, T, ByteBuffer> putType;

    private final Function<String, T> convertText;

    @Getter
    private volatile boolean open;

    @Getter
    private final Duration interval;

    @Getter
    private final Range<LocalDateTime> range;

    @Getter
    private final boolean readOnly;

    private long lastSet;

    private long lastSync;

    private int syncStart;

    private int syncEnd;

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
        this.bitShift = (int) Math.rint(Math.log(typeSize) / Math.log(2));
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
                throw new IllegalArgumentException(String.format("file (%s) size (%d) is not a valid multiple", file, numBytes));
            }

            this.readOnly = !file.canWrite();
            this.open = false;
            this.size = (int) (numBytes >> bitShift);
            this.interval = Duration.between(start, end).dividedBy(this.size);

        } else {

            this.readOnly = false;
            this.open = true;
            this.size = (int) Duration.between(start, end).dividedBy(interval);
            this.interval = interval;

            try {
                int numBytes = size << bitShift;
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

        clearSyncStatus();
    }

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
        int byteOffset = index << bitShift;
        return getType.apply(mappedByteBuffer, byteOffset);
    }

    @Override
    public T set(int index, T element) {
        log.debug("set: file={}, index={}, value={}", file, index, element);

        if (readOnly) {
            throw new IllegalStateException("file is read only");
        }

        long current = System.currentTimeMillis();
        lastSet = current;
        open();

        int byteOffset = index << bitShift;
        T previous = getType.apply(mappedByteBuffer, byteOffset);
        putType.apply(mappedByteBuffer, byteOffset, element);

        syncStart = Math.min(syncStart, byteOffset);
        syncEnd = Math.max(syncEnd, byteOffset + typeSize);

        if (lastSync < current - SYNC_TIME_MS) {
            sync();
        }

        return previous;
    }

    public void open() {
        if (!open) {
            synchronized (this) {
                if (!open) {
                    log.debug("open: file={}", file);
                    try {
                        randomAccessFile = new RandomAccessFile(file, readOnly ? "r" : "rw");
                        mappedByteBuffer = randomAccessFile.getChannel()
                            .map(this.readOnly ? READ_ONLY : READ_WRITE, 0, file.length());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    open = true;
                    clearSyncStatus();
                }
            }
        }
    }

    @Override
    public void close() {
        if (open) {
            synchronized (this) {
                if (open) {

                    sync();

                    log.debug("close: file={}", file);
                    try {
                        randomAccessFile.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    randomAccessFile = null;
                    mappedByteBuffer = null;
                    open = false;
                }
            }
        }
    }

    public void closeIfIdleOrSync() {
        if (open) {
            if (lastSet < System.currentTimeMillis() - IDLE_TIME_MS) {
                close();
            } else {
                sync();
            }
        }
    }

    private void sync() {
        if (syncEnd > syncStart) {
            log.debug("force: file={}, start={}, end={}", file, syncStart, syncEnd);
            mappedByteBuffer.force(syncStart, syncEnd - syncStart);
            clearSyncStatus();
        }
    }

    private void clearSyncStatus() {
        lastSync = System.currentTimeMillis();
        syncStart = Integer.MAX_VALUE;
        syncEnd = Integer.MIN_VALUE;
    }

}
