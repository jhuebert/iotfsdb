package org.huebert.iotfsdb.series;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.Getter;
import lombok.Setter;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.schema.FileInterval;
import org.huebert.iotfsdb.schema.Series;
import org.huebert.iotfsdb.series.adapter.SeriesTypeAdapter;

import java.io.File;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class SeriesContainer<T> {

    private final SeriesTypeAdapter<T> adapter;

    @Getter
    private final Series series;

    @Getter
    private final File seriesRoot;

    private final boolean readOnly;

    @Getter
    @Setter
    private Map<String, String> metadata;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final RangeMap<LocalDateTime, Supplier<SeriesFile<T>>> rangeMap = TreeRangeMap.create();

    public SeriesContainer(File seriesRoot, Series series, Map<String, String> metadata, SeriesTypeAdapter<T> adapter, boolean readOnly) {
        this.seriesRoot = Preconditions.checkNotNull(seriesRoot);
        this.series = Preconditions.checkNotNull(series);
        this.metadata = Preconditions.checkNotNull(metadata);
        this.adapter = Preconditions.checkNotNull(adapter);
        this.readOnly = readOnly;

        Preconditions.checkArgument(seriesRoot.exists());
        Preconditions.checkArgument(seriesRoot.isDirectory());
        Preconditions.checkArgument(seriesRoot.canRead());

        File[] values = seriesRoot.listFiles();
        if (values == null) {
            return;
        }

        FileInterval fileInterval = series.fileInterval();
        Stream.of(values)
            .filter(File::isFile)
            .filter(File::canRead)
            .filter(f -> fileInterval == FileInterval.findMatchingInterval(f.getName()))
            .forEach(f -> {
                LocalDateTime start = fileInterval.getStart(f.getName());
                Range<LocalDateTime> range = fileInterval.getRange(start);
                rangeMap.put(range, Suppliers.memoize(() -> {
                    FileBasedArray<T> array = adapter.readArray(f, series, start, readOnly, false);
                    return new SeriesFile<>(array, start, series.fileInterval());
                }));
            });
    }

    public Map<LocalDateTime, T> get(List<Range<LocalDateTime>> ranges, int valueInterval, boolean includeNull, SeriesAggregation aggregation) {
        Preconditions.checkNotNull(ranges);
        Preconditions.checkArgument(valueInterval > 0);

        Map<LocalDateTime, T> result = new LinkedHashMap<>();
        if (ranges.isEmpty()) {
            return result;
        }

        rwLock.readLock().lock();
        try {
            for (Range<LocalDateTime> current : ranges) {
                Stream<T> stream = rangeMap.subRangeMap(current)
                    .asMapOfRanges().values().stream()
                    .flatMap(f -> f.get().get(current).stream());
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
        Preconditions.checkNotNull(dateTime);
        Preconditions.checkArgument(!readOnly);

        Supplier<SeriesFile<T>> supplier;
        rwLock.readLock().lock();
        try {
            supplier = rangeMap.get(dateTime);
        } finally {
            rwLock.readLock().unlock();
        }

        T converted = adapter.convert(value);
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
                File file = new File(seriesRoot, filename);
                LocalDateTime start = fileInterval.getStart(filename);
                supplier = Suppliers.memoize(() -> {
                    FileBasedArray<T> array = adapter.readArray(file, series, start, false, true);
                    return new SeriesFile<>(array, start, fileInterval);
                });
                rangeMap.put(fileInterval.getRange(start), supplier);
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        supplier.get().set(dateTime, converted);
    }

}
