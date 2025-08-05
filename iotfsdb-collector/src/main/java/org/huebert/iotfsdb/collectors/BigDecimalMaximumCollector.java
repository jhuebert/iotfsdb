package org.huebert.iotfsdb.collectors;

import java.math.BigDecimal;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class BigDecimalMaximumCollector implements NumberCollector<BigDecimal, BigDecimalMaximumCollector.Maximum> {

    @Override
    public Supplier<Maximum> supplier() {
        return Maximum::new;
    }

    @Override
    public BiConsumer<Maximum, BigDecimal> accumulator() {
        return Maximum::accumulate;
    }

    @Override
    public BinaryOperator<Maximum> combiner() {
        return Maximum::combine;
    }

    @Override
    public Function<Maximum, Number> finisher() {
        return Maximum::get;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    public static class Maximum {

        private BigDecimal result = null;

        public void accumulate(BigDecimal value) {
            if ((result == null) || (value.compareTo(result) > 0)) {
                result = value;
            }
        }

        public Maximum combine(Maximum other) {
            accumulate(other.result);
            return this;
        }

        public BigDecimal get() {
            return result;
        }

    }

}
