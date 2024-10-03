package org.huebert.iotfsdb.series.adapter;

import com.google.common.math.Quantiles;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.file.FloatFileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.Stream;

public class FloatTypeAdapter implements SeriesTypeAdapter<Float> {

    private static final Map<SeriesAggregation, Function<Stream<Float>, Optional<Float>>> AGGREGATION_MAP = Map.of(
        SeriesAggregation.MINIMUM, s -> s.reduce(Float::min),
        SeriesAggregation.MAXIMUM, s -> s.reduce(Float::max),
        SeriesAggregation.FIRST, Stream::findFirst,
        SeriesAggregation.LAST, s -> s.reduce((a, b) -> b),
        SeriesAggregation.AVERAGE, FloatTypeAdapter::average,
        SeriesAggregation.MEDIAN, FloatTypeAdapter::median,
        SeriesAggregation.SUM, s -> s.reduce(Float::sum),
        SeriesAggregation.COUNT, s -> Optional.of((float) s.count())
    );

    @Override
    public FileBasedArray<Float> create(File file, int size) {
        return FloatFileBasedArray.create(file, size);
    }

    @Override
    public FileBasedArray<Float> read(File file, boolean readOnly) {
        return FloatFileBasedArray.read(file, readOnly);
    }

    @Override
    public Optional<Float> aggregate(Stream<Float> stream, SeriesAggregation aggregation) {
        return AGGREGATION_MAP.get(aggregation).apply(stream);
    }

    @Override
    public Float convert(String value) {
        return value == null ? null : Float.parseFloat(value);
    }

    private static Optional<Float> average(Stream<Float> stream) {
        OptionalDouble average = stream.mapToDouble(a -> a).average();
        if (average.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of((float) average.getAsDouble());
    }

    private static Optional<Float> median(Stream<Float> stream) {
        double[] array = stream.mapToDouble(a -> a).toArray();
        if (array.length < 1) {
            return Optional.empty();
        }
        return Optional.of((float) Quantiles.median().compute(array));
    }

}
