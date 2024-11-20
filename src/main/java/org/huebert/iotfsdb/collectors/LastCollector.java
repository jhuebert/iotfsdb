package org.huebert.iotfsdb.collectors;


import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class LastCollector implements NumberCollector<Number, LastCollector.Last> {

    @Override
    public Supplier<Last> supplier() {
        return Last::new;
    }

    @Override
    public BiConsumer<Last, Number> accumulator() {
        return Last::accumulate;
    }

    @Override
    public BinaryOperator<Last> combiner() {
        return Last::combine;
    }

    @Override
    public Function<Last, Number> finisher() {
        return Last::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of();
    }

    public static class Last {

        private Number result = null;

        public void accumulate(Number value) {
            result = value;
        }

        public Last combine(Last other) {
            return other;
        }

        public Number get() {
            return result;
        }

    }

}
