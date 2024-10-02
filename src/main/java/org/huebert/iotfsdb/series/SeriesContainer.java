package org.huebert.iotfsdb.series;

import com.google.common.base.Suppliers;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
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

    public Map<LocalDateTime, T> get(List<Range<LocalDateTime>> ranges, boolean includeNull, SeriesAggregation aggregation) {

        Map<LocalDateTime, T> result = new LinkedHashMap<>();
        if (ranges.isEmpty()) {
            return result;
        }

        rwLock.readLock().lock();
        try {
            for (Range<LocalDateTime> current : ranges) {
                Stream<T> stream = rangeMap.subRangeMap(current)
                    .asMapOfRanges().values().parallelStream() //TODO Will this cause an issue with first/last
                    .flatMap(f -> f.get().get(current).stream())
                    .filter(Objects::nonNull);
                T aggregate = adapter.aggregate(stream, aggregation);
                if (includeNull || (aggregate != null)) {
                    result.put(current.lowerEndpoint(), aggregate);
                }
            }
        } finally {
            rwLock.readLock().unlock();
        }

        return result;
    }

    public void set(LocalDateTime dateTime, String value) {

        T converted = adapter.convert(value);

        Supplier<SeriesFile<T>> supplier;
        rwLock.readLock().lock();
        try {
            supplier = rangeMap.get(dateTime);
        } finally {
            rwLock.readLock().unlock();
        }

        if (supplier != null) {
            supplier.get().set(dateTime, converted);
            return;
        }

        rwLock.writeLock().lock();
        try {
            supplier = rangeMap.get(dateTime);
            if (supplier == null) {
                FileInterval fileInterval = series.fileInterval();
                String filename = fileInterval.getFilename(dateTime);
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

        supplier.get().set(dateTime, converted);
    }

    @Override
    public void close() throws Exception {
        rwLock.writeLock().lock();
        try {
            for (Supplier<SeriesFile<T>> value : rangeMap.asMapOfRanges().values()) {
                value.get().close(); //TODO Not ideal that get is creating data before immediately closing it
            }
            rangeMap.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
