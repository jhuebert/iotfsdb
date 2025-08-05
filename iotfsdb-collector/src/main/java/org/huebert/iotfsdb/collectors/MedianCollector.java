package org.huebert.iotfsdb.collectors;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class MedianCollector implements NumberCollector<Double, List<Double>> {

    @Override
    public Supplier<List<Double>> supplier() {
        return ArrayList::new;
    }

    @Override
    public BiConsumer<List<Double>, Double> accumulator() {
        return List::add;
    }

    @Override
    public BinaryOperator<List<Double>> combiner() {
        return (a, b) -> {
            a.addAll(b);
            return a;
        };
    }

    @Override
    public Function<List<Double>, Number> finisher() {
        return a -> {

            if (a.isEmpty()) {
                return null;
            } else if (a.size() == 1) {
                return a.getFirst();
            } else if (a.size() == 2) {
                return (a.get(0) + a.get(1)) * 0.5;
            }

            a.sort(Comparator.naturalOrder());
            int index = a.size() / 2;
            double median = a.get(index);
            if (a.size() % 2 == 0) {
                median = (median + a.get(index - 1)) * 0.5;
            }
            return median;

        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

}
