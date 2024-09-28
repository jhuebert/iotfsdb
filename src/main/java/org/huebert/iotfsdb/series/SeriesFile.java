package org.huebert.iotfsdb.series;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.schema.FileInterval;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public class SeriesFile<T> {

    private final Duration interval;

    private final Range<LocalDateTime> dateTimeRange;

    private final FileBasedArray<T> fileBasedArray;

    protected SeriesFile(FileBasedArray<T> fileBasedArray, LocalDateTime start, FileInterval fileInterval) {
        this.fileBasedArray = Preconditions.checkNotNull(fileBasedArray);
        this.dateTimeRange = fileInterval.getRange(start);
        this.interval = fileInterval.getDuration(start).dividedBy(fileBasedArray.size());
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
        return (int) Duration.between(dateTimeRange.lowerEndpoint(), value).dividedBy(interval);
    }

}
