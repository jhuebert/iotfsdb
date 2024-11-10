package org.huebert.iotfsdb.collectors;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class MinimumCollector implements NumberCollector<Double, MinimumCollector.Minimum> {

    @Override
    public Supplier<Minimum> supplier() {
        return Minimum::new;
    }

    @Override
    public BiConsumer<Minimum, Double> accumulator() {
        return Minimum::accumulate;
    }

    @Override
    public BinaryOperator<Minimum> combiner() {
        return Minimum::combine;
    }

    @Override
    public Function<Minimum, Number> finisher() {
        return Minimum::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    public static class Minimum {

        private Double result = null;

        public void accumulate(Double value) {
            if ((result == null) || (value < result)) {
                result = value;
            }
        }

        public Minimum combine(Minimum other) {
            accumulate(other.result);
            return this;
        }

        public Number get() {
            return result;
        }

    }

}
