package org.huebert.iotfsdb.partition;

import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.math.Quantiles;
import lombok.experimental.UtilityClass;
import org.huebert.iotfsdb.partition.adapter.BytePartition;
import org.huebert.iotfsdb.partition.adapter.DoublePartition;
import org.huebert.iotfsdb.partition.adapter.FloatPartition;
import org.huebert.iotfsdb.partition.adapter.IntegerPartition;
import org.huebert.iotfsdb.partition.adapter.PartitionAdapter;
import org.huebert.iotfsdb.partition.adapter.ShortPartition;
import org.huebert.iotfsdb.series.NumberType;
import org.huebert.iotfsdb.series.Reducer;
import org.huebert.iotfsdb.series.SeriesDefinition;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

@UtilityClass
public class PartitionFactory {

    private static final Map<NumberType, PartitionAdapter> ADAPTER_MAP = Map.of(
        NumberType.FLOAT4, new FloatPartition(),
        NumberType.FLOAT8, new DoublePartition(),
        NumberType.INT1, new BytePartition(),
        NumberType.INT2, new ShortPartition(),
        NumberType.INT4, new IntegerPartition(),
        NumberType.INT8, new DoublePartition()
    );

    public static Partition create(SeriesDefinition definition, Path path, LocalDateTime start) {

        PartitionAdapter adapter = ADAPTER_MAP.get(definition.getType());
        if (adapter == null) {
            throw new IllegalArgumentException(String.format("series type %s not supported", definition.getType()));
        }

        Period period = definition.getPartition().getPeriod();
        Duration interval = Duration.ofSeconds(definition.getInterval());
        return new Partition(path, start, period, interval, adapter);
    }

    public static Optional<? extends Number> reduce(Stream<? extends Number> stream, Reducer reducer) {

        Stream<? extends Number> nonNullStream = stream.filter(Objects::nonNull);
        //TODO Have a boolean that allows the existence of any null to make the entire result null?

        if (reducer == Reducer.COUNT) {
            return Optional.of(nonNullStream.count());
        } else if (reducer == Reducer.FIRST) {
            return nonNullStream.findFirst();
        } else if (reducer == Reducer.LAST) {
            return nonNullStream.reduce((a, b) -> b);
        } else if (reducer == Reducer.COUNT_DISTINCT) {
            return Optional.of(nonNullStream.distinct().count());
        } else if (reducer == Reducer.MODE) {
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
        if (reducer == Reducer.AVERAGE) {
            result = doubleStream.average();
        } else if (reducer == Reducer.SUM) {
            result = OptionalDouble.of(doubleStream.sum());
        } else if (reducer == Reducer.MINIMUM) {
            result = doubleStream.min();
        } else if (reducer == Reducer.MAXIMUM) {
            result = doubleStream.max();
        } else if (reducer == Reducer.SQUARE_SUM) {
            result = OptionalDouble.of(doubleStream.map(v -> v * v).sum());
        } else if (reducer == Reducer.MEDIAN) {
            double[] array = doubleStream.toArray();
            if (array.length < 1) {
                result = OptionalDouble.empty();
            } else {
                result = OptionalDouble.of(Quantiles.median().compute(array));
            }
        } else {
            throw new IllegalArgumentException(String.format("reducer %s not supported", reducer));
        }

        if (result.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(result.getAsDouble());
    }
}
