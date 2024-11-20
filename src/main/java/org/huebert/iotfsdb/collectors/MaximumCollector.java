package org.huebert.iotfsdb.collectors;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class MaximumCollector implements NumberCollector<Double, MaximumCollector.Maximum> {

    @Override
    public Supplier<Maximum> supplier() {
        return Maximum::new;
    }

    @Override
    public BiConsumer<Maximum, Double> accumulator() {
        return Maximum::accumulate;
    }

    @Override
    public BinaryOperator<Maximum> combiner() {
        return Maximum::combine;
    }

    @Override
    public Function<Maximum, Number> finisher() {
        return Maximum::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    public static class Maximum {

        private Double result = null;

        public void accumulate(Double value) {
            if ((result == null) || (value > result)) {
                result = value;
            }
        }

        public Maximum combine(Maximum other) {
            accumulate(other.result);
            return this;
        }

        public Number get() {
            return result;
        }

    }

}
