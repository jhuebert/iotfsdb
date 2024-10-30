package org.huebert.iotfsdb.partition;

import com.google.common.collect.Range;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.partition.adapter.PartitionAdapter;
import org.huebert.iotfsdb.series.SeriesDefinition;
import org.huebert.iotfsdb.util.FileUtil;
import org.huebert.iotfsdb.util.GZIPOutputStream;
import org.huebert.iotfsdb.util.GzipUtil;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.zip.GZIPInputStream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

@Slf4j
public class Partition extends AbstractList<Number> implements RandomAccess, AutoCloseable {

    private static final OpenOption[] OPEN_OPTIONS_CREATE = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC, StandardOpenOption.CREATE_NEW};

    private static final OpenOption[] OPEN_OPTIONS_READ = new OpenOption[]{StandardOpenOption.READ};

    private static final OpenOption[] OPEN_OPTIONS_READ_WRITE = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC};

    @Getter
    private final Path path;

    @Getter
    private final long numBytes;

    @Getter
    private final Range<LocalDateTime> range;

    @Getter
    private volatile boolean open;

    @Getter
    private volatile boolean archive;

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
        SeriesDefinition definition,
        PartitionAdapter adapter
    ) {
        log.debug("Partition(enter): path={}, start={}, definition={}, typeSize={}", path, start, definition, adapter.getTypeSize());

        this.path = path;
        this.adapter = adapter;
        this.bitShift = adapter.getBitShift();

        Duration definitionInterval = Duration.ofSeconds(definition.getInterval());
        LocalDateTime end = start.plus(definition.getPartition().getPeriod());
        this.range = Range.closed(start, end.minusNanos(1));

        if (Files.exists(path)) {
            FileUtil.checkFile(path);

            try {
                archive = GzipUtil.isGZipped(path.toFile());
                if (archive) {
                    numBytes = GzipUtil.getUncompressedSize(path.toFile());
                } else {
                    numBytes = Files.size(path);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (numBytes == 0) {
                throw new IllegalArgumentException(String.format("file (%s) is empty", path));
            }

            if (!adapter.isMultiple(numBytes)) {
                throw new IllegalArgumentException(String.format("file (%s) size (%d) is not a valid multiple", path, numBytes));
            }

            this.readOnly = !Files.isWritable(path);
            this.open = false;
            this.idle = true;
            this.size = (int) (numBytes >> bitShift);

            Duration fileInterval = Duration.between(start, end).dividedBy(this.size);
            if (!Objects.equals(definitionInterval, fileInterval)) {
                log.debug("definition interval ({}) is not the same as the file's calculated interval ({})", definitionInterval, fileInterval);
            }

            // Need to use the calculated interval from the file as it may have been reduced
            this.interval = fileInterval;

        } else {

            this.archive = false;
            this.readOnly = false;
            this.open = true;
            this.idle = false;
            this.size = (int) Duration.between(start, end).dividedBy(definitionInterval);
            this.interval = definitionInterval;

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

        log.debug("Partition(exit): path={}, size={}, interval={}, bitShift={}, readOnly={}, range={}, open={}", this.path, size, this.interval, bitShift, readOnly, range, open);
    }

    @Override
    public int size() {
        return size;
    }

    public List<Number> get(Range<LocalDateTime> range) {
        log.debug("get(enter): path={}, range={}", path, range);
        Range<LocalDateTime> intersection = this.range.intersection(range);
        if (intersection.isEmpty()) {
            return List.of();
        }
        int fromIndex = getIndex(intersection.lowerEndpoint());
        int toIndex = getIndex(intersection.upperEndpoint());
        log.debug("get(exit): path={}, fromIndex={}, toIndex={}", path, fromIndex, toIndex);
        return subList(fromIndex, toIndex + 1);
    }

    public Number get(LocalDateTime dateTime) {
        log.debug("get(enter): path={}, dateTime={}", path, dateTime);
        int index = getIndex(dateTime);
        Number result = get(index);
        log.debug("get(exit): path={}, index={}, result={}", path, index, result);
        return result;
    }

    @Override
    public Number get(int index) {
        open();
        int byteOffset = index << bitShift;
        return adapter.get(mappedByteBuffer, byteOffset);
    }

    public void set(LocalDateTime dateTime, Number value) {
        log.debug("set(enter): path={}, dateTime={}, value={}", path, dateTime, value);
        int index = getIndex(dateTime);
        Number result = set(index, value);
        log.debug("set(exit): path={}, index={}, result={}", path, index, result);
    }

    @Override
    public Number set(int index, Number value) {
        log.debug("set(enter): path={}, index={}, value={}", path, index, value);

        if (archive || readOnly) {
            throw new IllegalStateException("file is read only");
        }

        open();

        int byteOffset = index << bitShift;
        Number previous = adapter.get(mappedByteBuffer, byteOffset);
        adapter.put(mappedByteBuffer, byteOffset, value);

        log.debug("set(exit): path={}, byteOffset={}, previous={}", path, byteOffset, previous);

        return previous;
    }

    public synchronized void archive() {

        close();

        if (archive || readOnly) {
            log.error("unable archive file");
            return;
        }

        try {
            Path tmp = Files.createTempFile("iotfsdb-", ".tmp");
            try (FileInputStream is = new FileInputStream(path.toFile())) {
                try (GZIPOutputStream os = new GZIPOutputStream(new FileOutputStream(tmp.toFile()))) {
                    is.transferTo(os);
                }
            }
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            archive = true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void unarchive() {

        close();

        if (!archive || readOnly) {
            log.error("unable unarchive file");
            return;
        }

        try {
            Path tmp = unzipToTemp(path);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            archive = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void open() {
        if (!open) {
            synchronized (this) {
                if (!open) {
                    log.debug("open: {}", path);

                    try {

                        Path toOpen = path;
                        if (archive) {
                            tempPath = unzipToTemp(path);
                            log.debug("uncompressed {} to temp file {}", path, tempPath);
                            toOpen = tempPath;
                        }

                        fileChannel = FileChannel.open(toOpen, archive || readOnly ? OPEN_OPTIONS_READ : OPEN_OPTIONS_READ_WRITE);
                        mappedByteBuffer = fileChannel.map(archive || readOnly ? READ_ONLY : READ_WRITE, 0, numBytes);

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    open = true;
                }
            }
        }
        idle = false;
    }

    private static Path unzipToTemp(Path path) {
        try {
            Path tmp = Files.createTempFile("iotfsdb-", ".tmp");
            try (GZIPInputStream is = new GZIPInputStream(new FileInputStream(path.toFile()))) {
                try (FileOutputStream os = new FileOutputStream(tmp.toFile())) {
                    is.transferTo(os);
                }
            }
            return tmp;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (open) {
            synchronized (this) {
                if (open) {
                    log.debug("close: {}", path);

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
                log.debug("setting idle: {}", path);
                idle = true;
            }
        }
        return result;
    }

    private int getIndex(LocalDateTime dateTime) {
        return (int) Duration.between(range.lowerEndpoint(), dateTime).dividedBy(interval);
    }

}
