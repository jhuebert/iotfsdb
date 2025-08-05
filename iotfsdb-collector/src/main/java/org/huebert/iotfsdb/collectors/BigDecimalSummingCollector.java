package org.huebert.iotfsdb.collectors;

import java.math.BigDecimal;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class BigDecimalSummingCollector implements NumberCollector<BigDecimal, BigDecimalSummingCollector.Sum> {

    @Override
    public Supplier<Sum> supplier() {
        return Sum::new;
    }

    @Override
    public BiConsumer<Sum, BigDecimal> accumulator() {
        return Sum::accumulate;
    }

    @Override
    public BinaryOperator<Sum> combiner() {
        return Sum::combine;
    }

    @Override
    public Function<Sum, Number> finisher() {
        return Sum::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    public static class Sum {

        private BigDecimal result = BigDecimal.ZERO;

        public void accumulate(BigDecimal value) {
            result = result.add(value);
        }

        public Sum combine(Sum other) {
            accumulate(other.result);
            return this;
        }

        public BigDecimal get() {
            return result;
        }

    }

}
