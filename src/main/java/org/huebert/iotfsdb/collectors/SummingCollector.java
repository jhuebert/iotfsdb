package org.huebert.iotfsdb.collectors;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class SummingCollector implements NumberCollector<Double, SummingCollector.Sum> {

    @Override
    public Supplier<Sum> supplier() {
        return Sum::new;
    }

    @Override
    public BiConsumer<Sum, Double> accumulator() {
        return Sum::accumulate;
    }

    @Override
    public BinaryOperator<Sum> combiner() {
        return Sum::combine;
    }

    @Override
    public Function<Sum, Number> finisher() {
        return Sum::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    public static class Sum {

        private double result = 0;

        public void accumulate(Double value) {
            result += value;
        }

        public Sum combine(Sum other) {
            accumulate(other.result);
            return this;
        }

        public Number get() {
            return result;
        }

    }

}
