package org.huebert.iotfsdb.series.adapter;

import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.file.FloatFileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;

import java.io.File;
import java.util.OptionalDouble;
import java.util.stream.Stream;

public class FloatTypeAdapter implements SeriesTypeAdapter<Float> {

    @Override
    public FileBasedArray<Float> create(File file, int size) {
        return FloatFileBasedArray.create(file, size);
    }

    @Override
    public FileBasedArray<Float> read(File file, boolean readOnly) {
        return FloatFileBasedArray.read(file, readOnly);
    }

    @Override
    public Float aggregate(Stream<Float> stream, SeriesAggregation aggregation) {
        OptionalDouble result = AGGREGATION_MAP.get(aggregation).apply(stream.mapToDouble(a -> (double) a));
        return result.isPresent() ? (float) result.getAsDouble() : null;
    }

    @Override
    public Float convert(String value) {
        return value == null ? null : Float.parseFloat(value);
    }

}
