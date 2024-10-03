package org.huebert.iotfsdb.series.adapter;

import com.google.common.math.Quantiles;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.file.IntegerFileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.Stream;

public class IntegerTypeAdapter implements SeriesTypeAdapter<Integer> {

    private static final Map<SeriesAggregation, Function<Stream<Integer>, Optional<Integer>>> AGGREGATION_MAP = Map.of(
        SeriesAggregation.MINIMUM, s -> s.reduce(Integer::min),
        SeriesAggregation.MAXIMUM, s -> s.reduce(Integer::max),
        SeriesAggregation.FIRST, Stream::findFirst,
        SeriesAggregation.LAST, s -> s.reduce((a, b) -> b),
        SeriesAggregation.AVERAGE, IntegerTypeAdapter::average,
        SeriesAggregation.MEDIAN, IntegerTypeAdapter::median,
        SeriesAggregation.SUM, s -> s.reduce(Integer::sum),
        SeriesAggregation.COUNT, s -> Optional.of((int) s.count())
    );

    @Override
    public FileBasedArray<Integer> create(File file, int size) {
        return IntegerFileBasedArray.create(file, size);
    }

    @Override
    public FileBasedArray<Integer> read(File file, boolean readOnly) {
        return IntegerFileBasedArray.read(file, readOnly);
    }

    @Override
    public Optional<Integer> aggregate(Stream<Integer> stream, SeriesAggregation aggregation) {
        return AGGREGATION_MAP.get(aggregation).apply(stream);
    }

    @Override
    public Integer convert(String value) {
        return value == null ? null : Integer.parseInt(value);
    }

    private static Optional<Integer> average(Stream<Integer> stream) {
        OptionalDouble average = stream.mapToInt(a -> a).average();
        if (average.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of((int) Math.rint(average.getAsDouble()));
    }

    private static Optional<Integer> median(Stream<Integer> stream) {
        int[] array = stream.mapToInt(a -> a).toArray();
        if (array.length < 1) {
            return Optional.empty();
        }
        return Optional.of((int) Math.rint(Quantiles.median().compute(array)));
    }

}
