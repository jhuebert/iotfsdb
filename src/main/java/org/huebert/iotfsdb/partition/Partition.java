package org.huebert.iotfsdb.partition;

import com.google.common.collect.Range;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.util.TriFunction;
import org.huebert.iotfsdb.util.Util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.BiFunction;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

@Slf4j
public abstract class Partition<T extends Number> extends AbstractList<T> implements RandomAccess, AutoCloseable {

    private static final OpenOption[] OPEN_OPTIONS_CREATE = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC, StandardOpenOption.CREATE_NEW};

    private static final OpenOption[] OPEN_OPTIONS_READ = new OpenOption[]{StandardOpenOption.READ};

    private static final OpenOption[] OPEN_OPTIONS_READ_WRITE = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC};

    private final File file;

    private final int size;

    private final int bitShift;

    private final Duration interval;

    private final boolean readOnly;

    private final BiFunction<ByteBuffer, Integer, T> getType;

    private final TriFunction<ByteBuffer, Integer, Number, ByteBuffer> putType;

    @Getter
    private final Range<LocalDateTime> range;

    @Getter
    private volatile boolean open;

    private volatile boolean idle;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    protected Partition(
        File file,
        LocalDateTime start,
        Period period,
        Duration interval,
        int typeSize,
        BiFunction<ByteBuffer, Integer, T> getType,
        TriFunction<ByteBuffer, Integer, Number, ByteBuffer> putType
    ) {
        log.debug("Partition(enter): file={}, start={}, period={}, interval={}, typeSize={}", file, start, period, interval, typeSize);

        this.bitShift = (int) Math.rint(Math.log(typeSize) / Math.log(2));
        this.getType = getType;
        this.putType = putType;
        this.file = file;
        this.interval = interval;

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
            this.idle = true;
            this.size = (int) (numBytes >> bitShift);

            Duration fileInterval = Duration.between(start, end).dividedBy(this.size);
            if (!Objects.equals(interval, fileInterval)) {
                log.warn("input interval ({}) is not the same as the file's calculated interval ({})", interval, fileInterval);
            }

        } else {

            this.readOnly = false;
            this.open = true;
            this.idle = false;
            this.size = (int) Duration.between(start, end).dividedBy(interval);

            try {
                int numBytes = size << bitShift;
                this.fileChannel = FileChannel.open(file.toPath(), OPEN_OPTIONS_CREATE);
                this.mappedByteBuffer = fileChannel.map(READ_WRITE, 0, numBytes);
                for (int i = 0; i < numBytes; i += typeSize) {
                    putType.apply(mappedByteBuffer, i, null);
                }
                mappedByteBuffer.rewind();
                mappedByteBuffer.force();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        log.debug("Partition(exit): file={}, size={}, bitShift={}, readOnly={}, range={}, open={}", this.file, size, bitShift, readOnly, range, open);
    }

    @Override
    public int size() {
        return size;
    }

    public List<T> get(Range<LocalDateTime> range) {
        log.debug("get(enter): file={}, range={}", file, range);
        Range<LocalDateTime> intersection = this.range.intersection(range);
        if (intersection.isEmpty()) {
            return List.of();
        }
        int fromIndex = getIndex(intersection.lowerEndpoint());
        int toIndex = getIndex(intersection.upperEndpoint());
        log.debug("get(exit): file={}, fromIndex={}, toIndex={}", file, fromIndex, toIndex);
        return subList(fromIndex, toIndex + 1);
    }

    public T get(LocalDateTime dateTime) {
        log.debug("get(enter): file={}, dateTime={}", file, dateTime);
        int index = getIndex(dateTime);
        T result = get(index);
        log.debug("get(exit): file={}, index={}, result={}", file, index, result);
        return result;
    }

    @Override
    public T get(int index) {
        open();
        int byteOffset = index << bitShift;
        return getType.apply(mappedByteBuffer, byteOffset);
    }

    public T set(LocalDateTime dateTime, Number value) {
        log.debug("set(enter): file={}, dateTime={}, value={}", file, dateTime, value);
        int index = getIndex(dateTime);
        T result = set(index, value);
        log.debug("set(exit): file={}, index={}, result={}", file, index, result);
        return result;
    }

    @Override
    public T set(int index, Number value) {
        log.debug("set(enter): file={}, index={}, value={}", file, index, value);

        if (readOnly) {
            throw new IllegalStateException("file is read only");
        }

        open();

        int byteOffset = index << bitShift;
        T previous = getType.apply(mappedByteBuffer, byteOffset);
        putType.apply(mappedByteBuffer, byteOffset, value);

        log.debug("set(exit): file={}, byteOffset={}, previous={}", file, byteOffset, previous);

        return previous;
    }

    public void open() {
        if (!open) {
            synchronized (this) {
                if (!open) {
                    log.debug("open: file={}", file);
                    try {
                        fileChannel = FileChannel.open(file.toPath(), this.readOnly ? OPEN_OPTIONS_READ : OPEN_OPTIONS_READ_WRITE);
                        mappedByteBuffer = fileChannel.map(this.readOnly ? READ_ONLY : READ_WRITE, 0, file.length());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    open = true;
                }
            }
        }
        idle = false;
    }

    @Override
    public void close() {
        if (open) {
            synchronized (this) {
                if (open) {
                    log.debug("close: file={}", file);

                    try {
                        fileChannel.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    mappedByteBuffer = null;
                    fileChannel = null;
                    open = false;
                }
            }
        }
    }

    public boolean closeIfIdle() {
        boolean result = false;
        if (open) {
            if (idle) {
                close();
                result = true;
            } else {
                idle = true;
            }
        }
        return result;
    }

    private int getIndex(LocalDateTime dateTime) {
        return (int) Duration.between(range.lowerEndpoint(), dateTime).dividedBy(interval);
    }

}
