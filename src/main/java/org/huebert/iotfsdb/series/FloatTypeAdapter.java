package org.huebert.iotfsdb.series;

import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.file.FloatFileBasedArray;
import org.huebert.iotfsdb.schema.Series;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.stream.Stream;

public class FloatTypeAdapter implements SeriesTypeAdapter<Float> {

    @Override
    public FileBasedArray<Float> readArray(File file, Series series, LocalDateTime start, boolean readOnly, boolean create) {
        if (!file.exists()) {
            int size = series.fileInterval().calculateSize(start, Duration.of(series.valueInterval(), ChronoUnit.SECONDS));
            return FloatFileBasedArray.create(file, size);
        }
        return FloatFileBasedArray.read(file, readOnly);
    }

    @Override
    public Float aggregate(Stream<Float> stream, SeriesAggregation aggregation) {
//        if (aggregation == SeriesAggregation.AVERAGE) {
        OptionalDouble result = stream.filter(Objects::nonNull).mapToDouble(a -> (double) a).average();
        return result.isPresent() ? (float) result.getAsDouble() : null;
//        }
    }
}
