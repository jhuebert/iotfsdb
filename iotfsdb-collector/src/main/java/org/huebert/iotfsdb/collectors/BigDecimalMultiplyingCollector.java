package org.huebert.iotfsdb.collectors;

import java.math.BigDecimal;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class BigDecimalMultiplyingCollector implements NumberCollector<BigDecimal, BigDecimalMultiplyingCollector.Result> {

    @Override
    public Supplier<Result> supplier() {
        return Result::new;
    }

    @Override
    public BiConsumer<Result, BigDecimal> accumulator() {
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

        private BigDecimal result = null;

        public void accumulate(BigDecimal value) {
            if (result == null) {
                result = value;
            } else {
                result = result.multiply(value);
            }
        }

        public Result combine(Result other) {
            accumulate(other.result);
            return this;
        }

        public BigDecimal get() {
            return result;
        }

    }

}
