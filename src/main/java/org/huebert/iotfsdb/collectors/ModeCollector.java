package org.huebert.iotfsdb.collectors;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public class ModeCollector implements NumberCollector<Number, Map<Number, AtomicInteger>> {

    @Override
    public Supplier<Map<Number, AtomicInteger>> supplier() {
        return HashMap::new;
    }

    @Override
    public BiConsumer<Map<Number, AtomicInteger>, Number> accumulator() {
        return (m, n) -> m.computeIfAbsent(n, k -> new AtomicInteger(1)).incrementAndGet();
    }

    @Override
    public BinaryOperator<Map<Number, AtomicInteger>> combiner() {
        return (a, b) -> {
            for (Map.Entry<Number, AtomicInteger> entry : b.entrySet()) {
                AtomicInteger count = a.get(entry.getKey());
                if (count != null) {
                    count.addAndGet(entry.getValue().get());
                } else {
                    a.put(entry.getKey(), entry.getValue());
                }
            }
            return a;
        };
    }

    @Override
    public Function<Map<Number, AtomicInteger>, Number> finisher() {
        return a -> a.entrySet().stream()
            .max(Comparator.comparing(e -> e.getValue().get()))
            .map(Map.Entry::getKey)
            .orElse(null);
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }

}
