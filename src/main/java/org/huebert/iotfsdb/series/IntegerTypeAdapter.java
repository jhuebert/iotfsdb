package org.huebert.iotfsdb.series;

import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.file.IntegerFileBasedArray;
import org.huebert.iotfsdb.rest.schema.Series;

import java.io.File;
import java.time.LocalDateTime;
import java.util.OptionalDouble;
import java.util.stream.Stream;

public class IntegerTypeAdapter implements SeriesTypeAdapter<Integer> {

    @Override
    public FileBasedArray<Integer> readArray(File file, Series series, LocalDateTime start, boolean readOnly, boolean create) {
        if (!file.exists()) {
            int size = series.fileInterval().calculateSize(start, series.valueInterval());
            return IntegerFileBasedArray.create(file, size);
        }
        return IntegerFileBasedArray.read(file, readOnly);
    }

    @Override
    public Integer aggregate(Stream<Integer> stream, SeriesAggregation aggregation) {
//        if (aggregation == SeriesAggregation.AVERAGE) {
        OptionalDouble result = stream.mapToDouble(a -> (double) a).average();
        return result.isPresent() ? (int) Math.rint(result.getAsDouble()) : null;
//        }
    }

}
