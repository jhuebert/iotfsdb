package org.huebert.iotfsdb.collectors;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class BigDecimalAveragingCollector implements NumberCollector<BigDecimal, BigDecimalAveragingCollector.CountAndSum> {

    @Override
    public Supplier<CountAndSum> supplier() {
        return CountAndSum::new;
    }

    @Override
    public BiConsumer<CountAndSum, BigDecimal> accumulator() {
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

        private BigDecimal sum = BigDecimal.ZERO;

        public void accumulate(BigDecimal value) {
            count++;
            sum = sum.add(value);
        }

        public CountAndSum combine(CountAndSum other) {
            count += other.count;
            sum = sum.add(other.sum);
            return this;
        }

        public BigDecimal get() {
            if (count == 0) {
                return null;
            }
            BigDecimal divisor = BigDecimal.valueOf(count);
            int precision = sum.precision() + (int) Math.ceil(10.0 * divisor.precision() / 3.0);
            return sum.divide(divisor, new MathContext(precision));
        }
    }

}
