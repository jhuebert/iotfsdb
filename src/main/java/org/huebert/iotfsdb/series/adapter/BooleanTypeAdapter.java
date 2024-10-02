package org.huebert.iotfsdb.series.adapter;

import org.huebert.iotfsdb.file.BooleanFileBasedArray;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;

import java.io.File;
import java.util.OptionalDouble;
import java.util.stream.Stream;

public class BooleanTypeAdapter implements SeriesTypeAdapter<Boolean> {

    @Override
    public FileBasedArray<Boolean> create(File file, int size) {
        return BooleanFileBasedArray.create(file, size);
    }

    @Override
    public FileBasedArray<Boolean> read(File file, boolean readOnly) {
        return BooleanFileBasedArray.read(file, readOnly);
    }

    @Override
    public Boolean aggregate(Stream<Boolean> stream, SeriesAggregation aggregation) {
        OptionalDouble result = AGGREGATION_MAP.get(aggregation).apply(stream.mapToDouble(a -> a ? 1.0 : 0.0));
        return result.isPresent() ? Math.rint(result.getAsDouble()) >= 0.5 : null;
    }

    @Override
    public Boolean convert(String value) {
        return value == null ? null : Boolean.parseBoolean(value);
    }

}
