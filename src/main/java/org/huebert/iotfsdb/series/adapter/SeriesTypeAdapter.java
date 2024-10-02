package org.huebert.iotfsdb.series.adapter;

import com.google.common.math.Quantiles;
import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;

import java.io.File;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

public interface SeriesTypeAdapter<T> {

    Map<SeriesAggregation, Function<DoubleStream, OptionalDouble>> AGGREGATION_MAP = Map.of(
        SeriesAggregation.MINIMUM, DoubleStream::min,
        SeriesAggregation.MAXIMUM, DoubleStream::max,
        SeriesAggregation.FIRST, DoubleStream::findFirst,
        SeriesAggregation.LAST, stream -> stream.reduce((a, b) -> b),
        SeriesAggregation.AVERAGE, DoubleStream::average,
        SeriesAggregation.MEDIAN, SeriesTypeAdapter::getMedian,
        SeriesAggregation.SUM, stream -> stream.reduce(Double::sum),
        SeriesAggregation.COUNT, stream -> OptionalDouble.of(stream.count())
    );

    FileBasedArray<T> create(File file, int size);

    FileBasedArray<T> read(File file, boolean readOnly);

    T aggregate(Stream<T> stream, SeriesAggregation aggregation);

    T convert(String value);

    private static OptionalDouble getMedian(DoubleStream stream) {
        double[] array = stream.toArray();
        if (array.length < 1) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(Quantiles.median().compute(array));
    }

}
