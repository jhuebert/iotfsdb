package org.huebert.iotfsdb.series;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import lombok.Getter;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.file.FloatFileBasedArray;
import org.huebert.iotfsdb.rest.schema.Series;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public abstract class SeriesFile<T> {

    @Getter
    private final Series series;

    @Getter
    private final LocalDateTime start;

    @Getter
    private final Duration interval;

    @Getter
    private final Range<LocalDateTime> dateTimeRange;

    private final FileBasedArray<T> fileBasedArray;

    protected SeriesFile(Series series, FileBasedArray<T> fileBasedArray, LocalDateTime start) {
        this.series = Preconditions.checkNotNull(series);
        this.fileBasedArray = Preconditions.checkNotNull(fileBasedArray);
        this.start = Preconditions.checkNotNull(start);
        this.interval = series.fileDuration().getDuration().dividedBy(fileBasedArray.size());
        this.dateTimeRange = Range.closedOpen(start, start.plus(series.fileDuration().getDuration()));
    }

    public T get(LocalDateTime dateTime) {
        Preconditions.checkArgument(dateTimeRange.contains(dateTime));
        return fileBasedArray.get(getIndex(dateTime));
    }

    public List<T> get(Range<LocalDateTime> range) {
        Range<LocalDateTime> intersection = dateTimeRange.intersection(range);
        if (intersection.isEmpty()) {
            return List.of();
        }
        return fileBasedArray.get(getIndex(range.lowerEndpoint()), getIndex(range.upperEndpoint()));
    }

    public void set(LocalDateTime dateTime, T value) {
        Preconditions.checkArgument(dateTimeRange.contains(dateTime));
        fileBasedArray.set(getIndex(dateTime), value);
    }

    private int getIndex(LocalDateTime value) {
        return (int) Duration.between(start, value).dividedBy(interval);
    }

    private record FileParameters(File file, int size) {
    }

    private static int calculateSize(Series series) {
        int size = (int) series.fileDuration().getDuration().dividedBy(series.valueDuration());
        Preconditions.checkArgument(size > 0);
        return size;
    }

    public static FloatSeries createFloatSeries(File file, Series series, LocalDateTime dateTime) {
        int size = calculateSize(series);
        FloatFileBasedArray fileBasedArray = FloatFileBasedArray.create(file, size);
        return new FloatSeries(series, fileBasedArray, dateTime);
    }

    public static FloatSeries readFloatSeries(File file, Series series, LocalDateTime dateTime, boolean readOnly) {
        FloatFileBasedArray fileBasedArray = FloatFileBasedArray.read(file, readOnly);
        return new FloatSeries(series, fileBasedArray, dateTime);
    }

    public static class FloatSeries extends SeriesFile<Float> {

        private FloatSeries(Series series, FileBasedArray<Float> fileBasedArray, LocalDateTime start) {
            super(series, fileBasedArray, start);
        }
    }

}
