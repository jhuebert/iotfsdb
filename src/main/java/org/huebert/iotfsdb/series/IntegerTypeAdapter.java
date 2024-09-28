package org.huebert.iotfsdb.series;

import org.apache.logging.log4j.util.Strings;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.file.IntegerFileBasedArray;
import org.huebert.iotfsdb.schema.Series;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.stream.Stream;

public class IntegerTypeAdapter implements SeriesTypeAdapter<Integer> {

    @Override
    public FileBasedArray<Integer> readArray(File file, Series series, LocalDateTime start, boolean readOnly, boolean create) {
        if (!file.exists()) {
            int size = series.fileInterval().calculateSize(start, Duration.of(series.valueInterval(), ChronoUnit.SECONDS));
            return IntegerFileBasedArray.create(file, size);
        }
        return IntegerFileBasedArray.read(file, readOnly);
    }

    @Override
    public Integer aggregate(Stream<Integer> stream, SeriesAggregation aggregation) {
//        if (aggregation == SeriesAggregation.AVERAGE) {
        OptionalDouble result = stream.filter(Objects::nonNull).mapToDouble(a -> (double) a).average();
        return result.isPresent() ? (int) Math.rint(result.getAsDouble()) : null;
//        }
    }

    @Override
    public Integer convert(String value) {
        if (Strings.isBlank(value)) {
            return null;
        }
        return Integer.parseInt(value);
    }

}
