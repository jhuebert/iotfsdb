package org.huebert.iotfsdb.collectors;


import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class CountingCollector implements NumberCollector<Number, CountingCollector.Count> {

    @Override
    public Supplier<Count> supplier() {
        return Count::new;
    }

    @Override
    public BiConsumer<Count, Number> accumulator() {
        return Count::accumulate;
    }

    @Override
    public BinaryOperator<Count> combiner() {
        return Count::combine;
    }

    @Override
    public Function<Count, Number> finisher() {
        return Count::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    public static class Count {

        private long count = 0;

        public void accumulate(Number value) {
            count++;
        }

        public Count combine(Count other) {
            count += other.count;
            return this;
        }

        public Number get() {
            return count;
        }

    }

}
