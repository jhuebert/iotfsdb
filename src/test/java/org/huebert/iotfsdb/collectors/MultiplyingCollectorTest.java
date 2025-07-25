package org.huebert.iotfsdb.collectors;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MultiplyingCollectorTest {

    @Test
    void testEmpty() {
        Stream<Double> stream = Stream.empty();
        assertThat(stream.parallel().collect(new MultiplyingCollector())).isEqualTo(null);
    }

    @Test
    void testValue1() {
        Stream<Double> stream = Stream.of(1000.0);
        assertThat(stream.parallel().collect(new MultiplyingCollector())).isEqualTo(1000.0);
    }

    @Test
    void testValue2() {
        Stream<Double> stream = Stream.of(500.0, 1000.0);
        assertThat(stream.parallel().collect(new MultiplyingCollector())).isEqualTo(500000.0);
    }

    @Test
    void testValue3() {
        Stream<Double> stream = Stream.of(300.0, 500.0, 1000.0);
        assertThat(stream.parallel().collect(new MultiplyingCollector())).isEqualTo(1.5E8);
    }

    @Test
    void testValueN() {
        Stream<Double> stream = IntStream.range(1, 10).mapToObj(a -> (double) a);
        assertThat(stream.parallel().collect(new MultiplyingCollector())).isEqualTo(362880.0);
    }

}
