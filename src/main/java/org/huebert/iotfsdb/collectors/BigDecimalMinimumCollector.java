package org.huebert.iotfsdb.collectors;

import java.math.BigDecimal;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class BigDecimalMinimumCollector implements NumberCollector<BigDecimal, BigDecimalMinimumCollector.Minimum> {

    @Override
    public Supplier<Minimum> supplier() {
        return Minimum::new;
    }

    @Override
    public BiConsumer<Minimum, BigDecimal> accumulator() {
        return Minimum::accumulate;
    }

    @Override
    public BinaryOperator<Minimum> combiner() {
        return Minimum::combine;
    }

    @Override
    public Function<Minimum, Number> finisher() {
        return Minimum::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    public static class Minimum {

        private BigDecimal result = null;

        public void accumulate(BigDecimal value) {
            if ((result == null) || (value.compareTo(result) < 0)) {
                result = value;
            }
        }

        public Minimum combine(Minimum other) {
            accumulate(other.result);
            return this;
        }

        public BigDecimal get() {
            return result;
        }

    }

}
