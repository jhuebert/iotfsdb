package org.huebert.iotfsdb.collectors;


import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;

import java.util.Comparator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class ModeCollector implements NumberCollector<Number, Multiset<Number>> {

    @Override
    public Supplier<Multiset<Number>> supplier() {
        return LinkedHashMultiset::create;
    }

    @Override
    public BiConsumer<Multiset<Number>, Number> accumulator() {
        return Multiset::add;
    }

    @Override
    public BinaryOperator<Multiset<Number>> combiner() {
        return (a, b) -> {
            a.addAll(b);
            return a;
        };
    }

    @Override
    public Function<Multiset<Number>, Number> finisher() {
        return a -> a.entrySet().stream()
            .max(Comparator.comparingInt(Multiset.Entry::getCount))
            .map(Multiset.Entry::getElement)
            .orElse(null);
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

}
