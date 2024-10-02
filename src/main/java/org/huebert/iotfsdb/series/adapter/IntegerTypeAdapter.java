package org.huebert.iotfsdb.series.adapter;

import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.file.IntegerFileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;

import java.io.File;
import java.util.OptionalDouble;
import java.util.stream.Stream;

public class IntegerTypeAdapter implements SeriesTypeAdapter<Integer> {

    @Override
    public FileBasedArray<Integer> create(File file, int size) {
        return IntegerFileBasedArray.create(file, size);
    }

    @Override
    public FileBasedArray<Integer> read(File file, boolean readOnly) {
        return IntegerFileBasedArray.read(file, readOnly);
    }

    @Override
    public Integer aggregate(Stream<Integer> stream, SeriesAggregation aggregation) {
        OptionalDouble result = AGGREGATION_MAP.get(aggregation).apply(stream.mapToDouble(a -> (double) a));
        return result.isPresent() ? (int) Math.rint(result.getAsDouble()) : null;
    }

    @Override
    public Integer convert(String value) {
        return value == null ? null : Integer.parseInt(value);
    }

}
