package org.huebert.iotfsdb.collectors;


import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class BigDecimalMedianCollector implements NumberCollector<BigDecimal, List<BigDecimal>> {

    private static final BigDecimal TWO = new BigDecimal("2");

    @Override
    public Supplier<List<BigDecimal>> supplier() {
        return ArrayList::new;
    }

    @Override
    public BiConsumer<List<BigDecimal>, BigDecimal> accumulator() {
        return List::add;
    }

    @Override
    public BinaryOperator<List<BigDecimal>> combiner() {
        return (a, b) -> {
            a.addAll(b);
            return a;
        };
    }

    @Override
    public Function<List<BigDecimal>, Number> finisher() {
        return a -> {

            if (a.isEmpty()) {
                return null;
            } else if (a.size() == 1) {
                return a.getFirst();
            } else if (a.size() == 2) {
                return average(a.get(0), a.get(1));
            }

            a.sort(Comparator.comparing(b -> b));
            int index = a.size() / 2;
            BigDecimal median = a.get(index);
            if (a.size() % 2 == 0) {
                return average(median, a.get(index - 1));
            }
            return median;

        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

    private static BigDecimal average(BigDecimal a, BigDecimal b) {
        BigDecimal sum = a.add(b);
        int precision = sum.precision() + (int) Math.ceil(10.0 * TWO.precision() / 3.0);
        return sum.divide(TWO, new MathContext(precision));
    }

}
