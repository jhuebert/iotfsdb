package org.huebert.iotfsdb.collectors;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class MultiplyingCollector implements NumberCollector<Double, MultiplyingCollector.Result> {

    @Override
    public Supplier<Result> supplier() {
        return Result::new;
    }

    @Override
    public BiConsumer<Result, Double> accumulator() {
        return Result::accumulate;
    }

    @Override
    public BinaryOperator<Result> combiner() {
        return Result::combine;
    }

    @Override
    public Function<Result, Number> finisher() {
        return Result::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    public static class Result {

        private Double result = null;

        public void accumulate(Double value) {
            if (result == null) {
                result = value;
            } else {
                result *= value;
            }
        }

        public Result combine(Result other) {
            accumulate(other.result);
            return this;
        }

        public Number get() {
            return result;
        }

    }

}
