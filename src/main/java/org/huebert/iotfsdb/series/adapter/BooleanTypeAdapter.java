package org.huebert.iotfsdb.series.adapter;

import org.huebert.iotfsdb.file.BooleanFileBasedArray;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.Stream;

public class BooleanTypeAdapter implements SeriesTypeAdapter<Boolean> {

    private static final Map<SeriesAggregation, Function<Stream<Boolean>, Optional<Boolean>>> AGGREGATION_MAP = Map.of(
        SeriesAggregation.MINIMUM, s -> s.reduce((a, b) -> a && b),
        SeriesAggregation.MAXIMUM, s -> s.reduce((a, b) -> a || b),
        SeriesAggregation.FIRST, Stream::findFirst,
        SeriesAggregation.LAST, s -> s.reduce((a, b) -> b),
        SeriesAggregation.AVERAGE, BooleanTypeAdapter::average,
        SeriesAggregation.MEDIAN, BooleanTypeAdapter::average,
        SeriesAggregation.SUM, s -> s.filter(a -> a).findAny(),
        SeriesAggregation.COUNT, s -> Optional.of(s.findAny().isPresent())
    );

    @Override
    public FileBasedArray<Boolean> create(File file, int size) {
        return BooleanFileBasedArray.create(file, size);
    }

    @Override
    public FileBasedArray<Boolean> read(File file, boolean readOnly) {
        return BooleanFileBasedArray.read(file, readOnly);
    }

    @Override
    public Optional<Boolean> aggregate(Stream<Boolean> stream, SeriesAggregation aggregation) {
        return AGGREGATION_MAP.get(aggregation).apply(stream);
    }

    @Override
    public Boolean convert(String value) {
        return value == null ? null : Boolean.parseBoolean(value);
    }

    private static Optional<Boolean> average(Stream<Boolean> stream) {
        OptionalDouble average = stream.mapToInt(a -> a ? 1 : 0).average();
        if (average.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(average.getAsDouble() >= 0.5);
    }

}
