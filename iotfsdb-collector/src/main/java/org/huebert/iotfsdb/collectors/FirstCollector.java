package org.huebert.iotfsdb.collectors;


import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class FirstCollector implements NumberCollector<Number, FirstCollector.First> {

    @Override
    public Supplier<First> supplier() {
        return First::new;
    }

    @Override
    public BiConsumer<First, Number> accumulator() {
        return First::accumulate;
    }

    @Override
    public BinaryOperator<First> combiner() {
        return First::combine;
    }

    @Override
    public Function<First, Number> finisher() {
        return First::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of();
    }

    public static class First {

        private Number result = null;

        public void accumulate(Number value) {
            if (result == null) {
                result = value;
            }
        }

        public First combine(First other) {
            return this;
        }

        public Number get() {
            return result;
        }

    }

}
