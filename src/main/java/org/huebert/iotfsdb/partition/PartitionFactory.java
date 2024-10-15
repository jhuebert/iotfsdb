package org.huebert.iotfsdb.partition;

import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.math.Quantiles;
import lombok.experimental.UtilityClass;
import org.huebert.iotfsdb.series.Aggregation;
import org.huebert.iotfsdb.series.SeriesType;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

@UtilityClass
public class PartitionFactory {

    public static Partition<?> create(SeriesType seriesType, File file, LocalDateTime start, Period period, Duration interval) {
        if (seriesType == SeriesType.BYTE) {
            return new BytePartition(file, start, period, interval);
        } else if (seriesType == SeriesType.DOUBLE) {
            return new DoublePartition(file, start, period, interval);
        } else if (seriesType == SeriesType.FLOAT) {
            return new FloatPartition(file, start, period, interval);
        } else if (seriesType == SeriesType.INTEGER) {
            return new IntegerPartition(file, start, period, interval);
        } else if (seriesType == SeriesType.LONG) {
            return new LongPartition(file, start, period, interval);
        } else if (seriesType == SeriesType.SHORT) {
            return new ShortPartition(file, start, period, interval);
        } else {
            throw new IllegalArgumentException(String.format("series type %s not supported", seriesType));
        }
    }

    public static Optional<? extends Number> aggregate(Stream<? extends Number> stream, Aggregation aggregation) {

        Stream<? extends Number> nonNullStream = stream.filter(Objects::nonNull);

        if (aggregation == Aggregation.COUNT) {
            return Optional.of(nonNullStream.count());
        } else if (aggregation == Aggregation.FIRST) {
            return nonNullStream.findFirst();
        } else if (aggregation == Aggregation.LAST) {
            return nonNullStream.reduce((a, b) -> b);
        } else if (aggregation == Aggregation.COUNT_DISTINCT) {
            return Optional.of(nonNullStream.distinct().count());
        } else if (aggregation == Aggregation.MODE) {
            List<? extends Number> list = nonNullStream.toList();
            if (list.isEmpty()) {
                return Optional.empty();
            } else if (list.size() < 3) {
                return list.stream().findFirst();
            }
            return LinkedHashMultiset.create(list).entrySet().stream()
                .max(Comparator.comparing(Multiset.Entry::getCount))
                .map(Multiset.Entry::getElement);
        }

        DoubleStream doubleStream = nonNullStream.mapToDouble(Number::doubleValue);

        OptionalDouble result;
        if (aggregation == Aggregation.AVERAGE) {
            result = doubleStream.average();
        } else if (aggregation == Aggregation.SUM) {
            result = OptionalDouble.of(doubleStream.sum());
        } else if (aggregation == Aggregation.MINIMUM) {
            result = doubleStream.min();
        } else if (aggregation == Aggregation.MAXIMUM) {
            result = doubleStream.max();
        } else if (aggregation == Aggregation.SQUARE_SUM) {
            result = OptionalDouble.of(doubleStream.map(v -> v * v).sum());
        } else if (aggregation == Aggregation.MEDIAN) {
            double[] array = doubleStream.toArray();
            if (array.length < 1) {
                result = OptionalDouble.empty();
            } else {
                result = OptionalDouble.of(Quantiles.median().compute(array));
            }
        } else {
            throw new IllegalArgumentException(String.format("aggregation %s not supported", aggregation));
        }

        if (result.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(result.getAsDouble());
    }
}
