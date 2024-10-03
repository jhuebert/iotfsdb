package org.huebert.iotfsdb.series;

import com.google.common.base.Suppliers;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.Util;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.schema.FileInterval;
import org.huebert.iotfsdb.schema.Series;
import org.huebert.iotfsdb.series.adapter.SeriesTypeAdapter;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Slf4j
public class SeriesContainer<T> implements AutoCloseable {

    private final SeriesTypeAdapter<T> adapter;

    @Getter
    private final Series series;

    @Getter
    private final File seriesRoot;

    @Getter
    @Setter
    private Map<String, String> metadata;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final RangeMap<LocalDateTime, Supplier<SeriesFile<T>>> rangeMap = TreeRangeMap.create();

    public SeriesContainer(File seriesRoot, Series series, Map<String, String> metadata, SeriesTypeAdapter<T> adapter, boolean readOnly) {
        this.seriesRoot = Util.checkDirectory(seriesRoot);
        this.series = series;
        this.metadata = metadata;
        this.adapter = adapter;

        File[] files = seriesRoot.listFiles();
        if (files == null) {
            throw new IllegalArgumentException(String.format("series directory (%s) contains no files", seriesRoot));
        }

        FileInterval fileInterval = series.fileInterval();
        Stream.of(files)
            .filter(File::isFile)
            .filter(file -> fileInterval == FileInterval.findMatch(file.getName()))
            .forEach(file -> {
                LocalDateTime start = fileInterval.getStart(file.getName());
                Range<LocalDateTime> range = fileInterval.getRange(start);
                rangeMap.put(range, Suppliers.memoize(() -> {
                    FileBasedArray<T> array = adapter.read(file, readOnly);
                    return new SeriesFile<>(array, start, series.fileInterval());
                }));
            });
    }

    @AllArgsConstructor
    private class Tuple {
        private final ZonedDateTime dateTime;
        private final T value;
    }

    public Map<ZonedDateTime, T> get(List<Range<ZonedDateTime>> ranges, boolean includeNull, SeriesAggregation aggregation) {

        Map<ZonedDateTime, T> result = new LinkedHashMap<>();
        if (ranges.isEmpty()) {
            return result;
        }

        rwLock.readLock().lock();
        try {
            ranges.parallelStream()
                .map(current -> {
                    Range<LocalDateTime> local = convertToUtc(current);
                    Stream<T> stream = rangeMap.subRangeMap(local).asMapOfRanges().values().stream()
                        .flatMap(f -> f.get().get(local).stream())
                        .filter(Objects::nonNull);
                    return new Tuple(current.lowerEndpoint(), adapter.aggregate(stream, aggregation).orElse(null));
                })
                .filter(t -> includeNull || (t.value != null))
                .forEachOrdered(t -> result.put(t.dateTime, t.value));
        } finally {
            rwLock.readLock().unlock();
        }

        return result;
    }

    private LocalDateTime convertToUtc(ZonedDateTime zonedDateTime) {
        return zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private Range<LocalDateTime> convertToUtc(Range<ZonedDateTime> zonedRange) {
        return Range.range(
            convertToUtc(zonedRange.lowerEndpoint()),
            zonedRange.lowerBoundType(),
            convertToUtc(zonedRange.upperEndpoint()),
            zonedRange.upperBoundType()
        );
    }

    public void set(ZonedDateTime dateTime, String value) {

        LocalDateTime local = convertToUtc(dateTime);
        T converted = adapter.convert(value);

        Supplier<SeriesFile<T>> supplier;
        rwLock.readLock().lock();
        try {
            supplier = rangeMap.get(local);
        } finally {
            rwLock.readLock().unlock();
        }

        if (supplier != null) {
            supplier.get().set(local, converted);
            return;
        }

        rwLock.writeLock().lock();
        try {
            supplier = rangeMap.get(local);
            if (supplier == null) {
                FileInterval fileInterval = series.fileInterval();
                String filename = fileInterval.getFilename(local);
                LocalDateTime start = fileInterval.getStart(filename);
                supplier = Suppliers.memoize(() -> {
                    File file = new File(Util.checkDirectory(seriesRoot), filename);
                    int size = series.calculateSize(start);
                    FileBasedArray<T> array = adapter.create(file, size);
                    return new SeriesFile<>(array, start, fileInterval);
                });
                rangeMap.put(fileInterval.getRange(start), supplier);
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        supplier.get().set(local, converted);
    }

    @Override
    public void close() throws Exception {
        rwLock.writeLock().lock();
        try {
            for (Supplier<SeriesFile<T>> value : rangeMap.asMapOfRanges().values()) {
                value.get().close();
            }
            rangeMap.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
