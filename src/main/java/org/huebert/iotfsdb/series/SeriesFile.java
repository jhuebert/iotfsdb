package org.huebert.iotfsdb.series;

import com.google.common.collect.Range;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.schema.FileInterval;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public class SeriesFile<T> implements AutoCloseable {

    private final Duration interval;

    private final Range<LocalDateTime> dateTimeRange;

    private final FileBasedArray<T> fileBasedArray;

    public SeriesFile(FileBasedArray<T> fileBasedArray, LocalDateTime start, FileInterval fileInterval) {
        this.fileBasedArray = fileBasedArray;
        this.dateTimeRange = fileInterval.getRange(start);
        this.interval = fileInterval.getDuration(start).dividedBy(fileBasedArray.size());
    }

    public List<T> get(Range<LocalDateTime> range) {
        Range<LocalDateTime> intersection = dateTimeRange.intersection(range);
        if (intersection.isEmpty()) {
            return List.of();
        }
        int start = calculateNumIntervals(dateTimeRange.lowerEndpoint(), intersection.lowerEndpoint());
        int length = calculateNumIntervals(intersection.lowerEndpoint(), intersection.upperEndpoint());
        return fileBasedArray.get(start, length);
    }

    public void set(LocalDateTime dateTime, T value) {
        int index = calculateNumIntervals(dateTimeRange.lowerEndpoint(), dateTime);
        fileBasedArray.set(index, value);
    }

    private int calculateNumIntervals(LocalDateTime start, LocalDateTime end) {
        return (int) Duration.between(start, end).dividedBy(interval);
    }

    @Override
    public void close() throws Exception {
        fileBasedArray.close();
    }
}
