package org.huebert.iotfsdb.partition;

import com.google.common.collect.Range;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.partition.adapter.PartitionAdapter;
import org.huebert.iotfsdb.util.Util;

import java.io.IOException;
import java.net.URI;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

@Slf4j
public class Partition extends AbstractList<Number> implements RandomAccess, AutoCloseable {

    private static final OpenOption[] OPEN_OPTIONS_CREATE = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC, StandardOpenOption.CREATE_NEW};

    private static final OpenOption[] OPEN_OPTIONS_READ = new OpenOption[]{StandardOpenOption.READ};

    private static final OpenOption[] OPEN_OPTIONS_READ_WRITE = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC};

    private static final Map<String, String> CREATE_ZIP = Map.of("create", "true");

    @Getter
    private final URI uri;

    @Getter
    private final long numBytes;

    @Getter
    private final Range<LocalDateTime> range;

    @Getter
    private volatile boolean open;

    private final int size;

    private final int bitShift;

    private final Duration interval;

    private final boolean readOnly;

    private final PartitionAdapter adapter;

    private volatile boolean idle;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private Path tempPath;

    protected Partition(
        Path path,
        LocalDateTime start,
        Period period,
        Duration interval,
        PartitionAdapter adapter
    ) {
        log.debug("Partition(enter): uri={}, start={}, period={}, interval={}, typeSize={}", path, start, period, interval, adapter.getTypeSize());

        this.bitShift = (int) Math.rint(Math.log(adapter.getTypeSize()) / Math.log(2));
        this.uri = path.toUri();
        this.interval = interval;
        this.adapter = adapter;

        LocalDateTime end = start.plus(period);
        this.range = Range.closed(start, end.minusNanos(1));

        if (Files.exists(path)) {
            Util.checkFile(path);

            numBytes = Util.size(path);
            if (numBytes == 0) {
                throw new IllegalArgumentException(String.format("file (%s) is empty", uri));
            }

            if (numBytes % adapter.getTypeSize() != 0) {
                throw new IllegalArgumentException(String.format("file (%s) size (%d) is not a valid multiple", uri, numBytes));
            }

            this.readOnly = uri.getScheme().equals("jar") || !Files.isWritable(path);
            this.open = false;
            this.idle = true;
            this.size = (int) (numBytes >> bitShift);

            Duration fileInterval = Duration.between(start, end).dividedBy(this.size);
            if (!Objects.equals(interval, fileInterval)) {
                log.debug("input interval ({}) is not the same as the file's calculated interval ({})", interval, fileInterval);
            }

        } else {

            this.readOnly = false;
            this.open = true;
            this.idle = false;
            this.size = (int) Duration.between(start, end).dividedBy(interval);

            try {
                numBytes = (long) size << bitShift;
                this.fileChannel = FileChannel.open(path, OPEN_OPTIONS_CREATE);
                this.mappedByteBuffer = fileChannel.map(READ_WRITE, 0, numBytes);
                for (int i = 0; i < numBytes; i += adapter.getTypeSize()) {
                    adapter.put(mappedByteBuffer, i, null);
                }
                mappedByteBuffer.rewind();
                mappedByteBuffer.force();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        log.debug("Partition(exit): uri={}, size={}, bitShift={}, readOnly={}, range={}, open={}", this.uri, size, bitShift, readOnly, range, open);
    }

    @Override
    public int size() {
        return size;
    }

    public List<Number> get(Range<LocalDateTime> range) {
        log.debug("get(enter): uri={}, range={}", uri, range);
        Range<LocalDateTime> intersection = this.range.intersection(range);
        if (intersection.isEmpty()) {
            return List.of();
        }
        int fromIndex = getIndex(intersection.lowerEndpoint());
        int toIndex = getIndex(intersection.upperEndpoint());
        log.debug("get(exit): uri={}, fromIndex={}, toIndex={}", uri, fromIndex, toIndex);
        return subList(fromIndex, toIndex + 1);
    }

    public Number get(LocalDateTime dateTime) {
        log.debug("get(enter): uri={}, dateTime={}", uri, dateTime);
        int index = getIndex(dateTime);
        Number result = get(index);
        log.debug("get(exit): uri={}, index={}, result={}", uri, index, result);
        return result;
    }

    @Override
    public Number get(int index) {
        open();
        int byteOffset = index << bitShift;
        return adapter.get(mappedByteBuffer, byteOffset);
    }

    public Number set(LocalDateTime dateTime, Number value) {
        log.debug("set(enter): uri={}, dateTime={}, value={}", uri, dateTime, value);
        int index = getIndex(dateTime);
        Number result = set(index, value);
        log.debug("set(exit): uri={}, index={}, result={}", uri, index, result);
        return result;
    }

    @Override
    public Number set(int index, Number value) {
        log.debug("set(enter): uri={}, index={}, value={}", uri, index, value);

        if (readOnly) {
            throw new IllegalStateException("file is read only");
        }

        open();

        int byteOffset = index << bitShift;
        Number previous = adapter.get(mappedByteBuffer, byteOffset);
        adapter.put(mappedByteBuffer, byteOffset, value);

        log.debug("set(exit): uri={}, byteOffset={}, previous={}", uri, byteOffset, previous);

        return previous;
    }

    public void open() {
        if (!open) {
            synchronized (this) {
                if (!open) {
                    log.debug("open: {}", uri);

                    try {

                        Path toOpen;
                        if (uri.getScheme().equals("jar")) {
                            tempPath = Files.createTempFile("iotfsdb-partition-", "");
                            try (FileSystem ignored = FileSystems.newFileSystem(uri, CREATE_ZIP)) {
                                log.debug("copying {} to temp file {}", uri, tempPath);
                                toOpen = Files.copy(Path.of(uri), tempPath, StandardCopyOption.REPLACE_EXISTING);
                            }
                        } else {
                            toOpen = Path.of(uri);
                        }

                        fileChannel = FileChannel.open(toOpen, this.readOnly ? OPEN_OPTIONS_READ : OPEN_OPTIONS_READ_WRITE);
                        mappedByteBuffer = fileChannel.map(this.readOnly ? READ_ONLY : READ_WRITE, 0, numBytes);

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
                    log.debug("close: {}", uri);

                    try {
                        fileChannel.close();
                        if ((tempPath != null) && Files.deleteIfExists(tempPath)) {
                            log.debug("deleted temp file: {}", tempPath);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    tempPath = null;
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
                log.debug("setting idle: {}", uri);
                idle = true;
            }
        }
        return result;
    }

    private int getIndex(LocalDateTime dateTime) {
        return (int) Duration.between(range.lowerEndpoint(), dateTime).dividedBy(interval);
    }

}
