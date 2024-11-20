package org.huebert.iotfsdb.collectors;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class AveragingCollector implements NumberCollector<Double, AveragingCollector.CountAndSum> {

    @Override
    public Supplier<CountAndSum> supplier() {
        return CountAndSum::new;
    }

    @Override
    public BiConsumer<CountAndSum, Double> accumulator() {
        return CountAndSum::accumulate;
    }

    @Override
    public BinaryOperator<CountAndSum> combiner() {
        return CountAndSum::combine;
    }

    @Override
    public Function<CountAndSum, Number> finisher() {
        return CountAndSum::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of();
    }

    public static class CountAndSum {

        private long count = 0;

        private double sum = 0;

        public void accumulate(double value) {
            count++;
            sum += value;
        }

        public CountAndSum combine(CountAndSum other) {
            count += other.count;
            sum += other.sum;
            return this;
        }

        public Number get() {
            if (count == 0) {
                return null;
            }
            return sum / count;
        }

    }

}
