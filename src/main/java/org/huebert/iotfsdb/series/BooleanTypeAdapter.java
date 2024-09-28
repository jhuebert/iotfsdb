package org.huebert.iotfsdb.series;

import org.huebert.iotfsdb.file.BooleanFileBasedArray;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.schema.DataValue;
import org.huebert.iotfsdb.schema.Series;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.stream.Stream;

public class BooleanTypeAdapter implements SeriesTypeAdapter<Boolean> {

    @Override
    public FileBasedArray<Boolean> readArray(File file, Series series, LocalDateTime start, boolean readOnly, boolean create) {
        if (!file.exists()) {
            int size = series.fileInterval().calculateSize(start, Duration.of(series.valueInterval(), ChronoUnit.SECONDS));
            return BooleanFileBasedArray.create(file, size);
        }
        return BooleanFileBasedArray.read(file, readOnly);
    }

    @Override
    public Boolean aggregate(Stream<Boolean> stream, SeriesAggregation aggregation) {
//        if (aggregation == SeriesAggregation.AVERAGE) {
        OptionalDouble result = stream.filter(Objects::nonNull).mapToDouble(a -> a ? 1.0 : 0.0).average();
        return result.isPresent() ? result.getAsDouble() >= 0.5 : null;
//        }
    }

    @Override
    public Boolean convert(DataValue value) {
        return value.getBooleanValue();
    }

}
