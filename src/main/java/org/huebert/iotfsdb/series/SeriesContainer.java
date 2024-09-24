package org.huebert.iotfsdb.series;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.huebert.iotfsdb.rest.schema.Series;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.OptionalDouble;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class SeriesContainer<T> {

    private final Series series;

    private final File seriesRoot;

    private final boolean readOnly;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final RangeMap<LocalDateTime, Supplier<SeriesFile.FloatSeries>> rangeMap = TreeRangeMap.create();

    public SeriesContainer(File dbRoot, Series series, boolean readOnly) {
        this.series = series;
        //TODO Load all the files?
        File seriesRoot = new File(dbRoot, series.id());
//        File[] files = seriesRoot.listFiles(pathname -> pathname.isFile() && !pathname.canRead());
        List<File> list = Stream.of(seriesRoot.listFiles())
            .filter(File::isFile)
            .filter(File::canRead)
            .filter(f -> {
                try {
                    series.fileDuration().getFormatter().parse(f.getName());
                    return true;
                } catch (DateTimeParseException e) {
                    return false;
                }
            })
            .toList();

        for (File file : list) {
            LocalDateTime start = series.fileDuration().getFormatter().parse(file.getName());
            Range<LocalDateTime> range = Range.closedOpen(start, start.plus(series.fileDuration().getDuration()));
            rangeMap.put(range, Suppliers.memoize(() -> SeriesFile.readFloatSeries(file, series, start, readOnly)));
        }

    }

    public SortedMap<LocalDateTime, Float> get(Range<LocalDateTime> range, Duration interval) {
        Preconditions.checkNotNull(range);
        Preconditions.checkArgument(range.hasLowerBound());
        Preconditions.checkArgument(range.hasUpperBound());
        Preconditions.checkNotNull(interval);

        SortedMap<LocalDateTime, Float> result = new TreeMap<>();
        if (range.isEmpty()) {
            return result;
        }

        rwLock.readLock().lock();
        try {

            //TODO Add different aggregations

            long count = Duration.between(range.lowerEndpoint(), range.upperEndpoint()).dividedBy(interval);

            LocalDateTime start = range.lowerEndpoint();
            for (int i = 0; i < count; i++) {

                LocalDateTime end = start.plus(interval);

                Range<LocalDateTime> current = Range.closedOpen(start, end);

                OptionalDouble average = rangeMap.subRangeMap(current)
                    .asMapOfRanges().values().stream()
                    .flatMap(f -> f.get().get(current).stream())
                    .mapToDouble(a -> (double) a)
                    .average();

                result.put(start, average.isPresent() ? (float) average.getAsDouble() : null);

                start = end;
            }

            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Float get(LocalDateTime dateTime) {

        Supplier<SeriesFile.FloatSeries> supplier;
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

    public void set(LocalDateTime dateTime, Float value) {
        Preconditions.checkArgument(!readOnly);

        Supplier<SeriesFile.FloatSeries> supplier;
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
                File file = new File(seriesRoot, series.fileDuration().getFilename(dateTime));
                LocalDateTime start = getStart(dateTime);
                supplier = () -> SeriesFile.createFloatSeries(file, series, start);
                rangeMap.put(Range.closedOpen(start, start.plus(series.fileDuration().getDuration())), supplier);
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        supplier.get().set(dateTime, value);
    }

    private LocalDateTime getStart(String filename) {
        series.fileDuration().
    }

}
