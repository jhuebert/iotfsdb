package org.huebert.iotfsdb.series;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.Getter;
import lombok.Setter;
import org.huebert.iotfsdb.schema.FileInterval;
import org.huebert.iotfsdb.schema.Series;
import org.huebert.iotfsdb.series.adapter.SeriesTypeAdapter;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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
        Preconditions.checkNotNull(seriesRoot);
        Preconditions.checkArgument(seriesRoot.exists());
        Preconditions.checkArgument(seriesRoot.isDirectory());
        Preconditions.checkArgument(seriesRoot.canRead());
        Preconditions.checkNotNull(series);
        Preconditions.checkNotNull(adapter);

        this.series = series;
        this.seriesRoot = seriesRoot;
        this.readOnly = readOnly;
        this.adapter = adapter;
        this.metadata = metadata;

        FileInterval fileInterval = series.fileInterval();
        Stream.of(seriesRoot.listFiles())
            .filter(File::isFile)
            .filter(File::canRead)
            .filter(f -> fileInterval == FileInterval.findMatchingInterval(f.getName()))
            .forEach(f -> {
                LocalDateTime start = fileInterval.getStart(f.getName());
                Range<LocalDateTime> range = fileInterval.getRange(start);
                rangeMap.put(range, Suppliers.memoize(() -> new SeriesFile<>(adapter.readArray(f, series, start, readOnly, false), start, series.fileInterval())));
            });
    }

    public Map<LocalDateTime, T> get(Range<LocalDateTime> range, int valueInterval, boolean includeNull) {
        Preconditions.checkNotNull(range);
        Preconditions.checkArgument(range.hasLowerBound());
        Preconditions.checkArgument(range.hasUpperBound());
        Preconditions.checkArgument(valueInterval > 0);

        Map<LocalDateTime, T> result = new LinkedHashMap<>();
        if (range.isEmpty()) {
            return result;
        }

        Duration valueDuration = Duration.of(valueInterval, ChronoUnit.SECONDS);
        int count = (int) Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(valueDuration);
        List<Range<LocalDateTime>> ranges = new ArrayList<>(count);
        LocalDateTime start = range.lowerEndpoint();
        for (int i = 0; i <= count; i++) {
            LocalDateTime end = start.plus(valueDuration);
            ranges.add(Range.closedOpen(start, end));
            start = end;
        }

        rwLock.readLock().lock();
        try {
            for (Range<LocalDateTime> current : ranges) {
                //TODO Add different aggregations
                Stream<T> stream = rangeMap.subRangeMap(current)
                    .asMapOfRanges().values().stream()
                    .flatMap(f -> f.get().get(current).stream());
                T aggregate = adapter.aggregate(stream, SeriesAggregation.AVERAGE);
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
        Preconditions.checkArgument(!readOnly);

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
                File file = new File(seriesRoot, filename);
                LocalDateTime start = fileInterval.getStart(filename);
                supplier = Suppliers.memoize(() -> new SeriesFile<>(adapter.readArray(file, series, start, false, true), start, fileInterval));
                rangeMap.put(fileInterval.getRange(start), supplier);
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        supplier.get().set(dateTime, converted);
    }

}
