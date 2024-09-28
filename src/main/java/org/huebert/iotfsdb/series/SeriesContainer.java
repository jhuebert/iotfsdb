package org.huebert.iotfsdb.series;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.Getter;
import org.huebert.iotfsdb.rest.schema.FileInterval;
import org.huebert.iotfsdb.rest.schema.Series;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class SeriesContainer<T> {

    private final SeriesTypeAdapter<T> adapter;

    @Getter
    private final Series series;

    private final File seriesRoot;

    private final boolean readOnly;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final RangeMap<LocalDateTime, Supplier<SeriesFile<T>>> rangeMap = TreeRangeMap.create();

    public SeriesContainer(File seriesRoot, Series series, SeriesTypeAdapter<T> adapter, boolean readOnly) {
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

    public SortedMap<LocalDateTime, T> get(Range<LocalDateTime> range, Duration interval) {
        Preconditions.checkNotNull(range);
        Preconditions.checkArgument(range.hasLowerBound());
        Preconditions.checkArgument(range.hasUpperBound());
        Preconditions.checkNotNull(interval);

        SortedMap<LocalDateTime, T> result = new TreeMap<>();
        if (range.isEmpty()) {
            return result;
        }

        int count = (int) Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(interval);
        List<Range<LocalDateTime>> ranges = new ArrayList<>(count);
        LocalDateTime start = range.lowerEndpoint();
        for (int i = 0; i < count; i++) {
            LocalDateTime end = start.plus(interval);
            ranges.add(Range.closedOpen(start, end));
            start = end;
        }

        rwLock.readLock().lock();
        try {
            for (Range<LocalDateTime> current : ranges) {
                //TODO Add different aggregations
                T aggregate = adapter.aggregate(rangeMap.subRangeMap(current)
                    .asMapOfRanges().values().stream()
                    .flatMap(f -> f.get().get(current).stream()), SeriesAggregation.AVERAGE);
                result.put(current.lowerEndpoint(), aggregate); //TODO Tree maps don't allow null values, linked map?
            }
        } finally {
            rwLock.readLock().unlock();
        }

        return result;
    }

    public T get(LocalDateTime dateTime) {

        Supplier<SeriesFile<T>> supplier;
        rwLock.readLock().lock();
        try {
            supplier = rangeMap.get(dateTime);
        } finally {
            rwLock.readLock().unlock();
        }

        if (supplier == null) {
            return null;
        }
        return supplier.get().get(dateTime);
    }

    public void set(LocalDateTime dateTime, T value) {
        Preconditions.checkArgument(!readOnly);

        Supplier<SeriesFile<T>> supplier;
        rwLock.readLock().lock();
        try {
            supplier = rangeMap.get(dateTime);
        } finally {
            rwLock.readLock().unlock();
        }

        if (supplier != null) {
            supplier.get().set(dateTime, value);
            return;
        }

        rwLock.writeLock().lock();
        try {
            supplier = rangeMap.get(dateTime);
            if (supplier == null) {
                File file = new File(seriesRoot, series.fileInterval().getFilename(dateTime));
                LocalDateTime start = series.fileInterval().getStart(dateTime);
                supplier = Suppliers.memoize(() -> new SeriesFile<>(adapter.readArray(file, series, start, false, true), start, series.fileInterval()));
                rangeMap.put(series.fileInterval().getRange(start), supplier);
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        supplier.get().set(dateTime, value);
    }

}
