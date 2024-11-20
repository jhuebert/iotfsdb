package org.huebert.iotfsdb.collectors;


import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class CountingDistinctCollector implements NumberCollector<Number, Set<Number>> {

    @Override
    public Supplier<Set<Number>> supplier() {
        return HashSet::new;
    }

    @Override
    public BiConsumer<Set<Number>, Number> accumulator() {
        return Set::add;
    }

    @Override
    public BinaryOperator<Set<Number>> combiner() {
        return (a, b) -> {
            a.addAll(b);
            return a;
        };
    }

    @Override
    public Function<Set<Number>, Number> finisher() {
        return Set::size;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

}
