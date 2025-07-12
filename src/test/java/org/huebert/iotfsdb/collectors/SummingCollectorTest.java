package org.huebert.iotfsdb.collectors;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SummingCollectorTest {

    @Test
    void testEmpty() {
        Stream<Double> stream = Stream.empty();
        assertThat(stream.parallel().collect(new SummingCollector())).isEqualTo(0.0);
    }

    @Test
    void testValue1() {
        Stream<Double> stream = Stream.of(1000.0);
        assertThat(stream.parallel().collect(new SummingCollector())).isEqualTo(1000.0);
    }

    @Test
    void testValue2() {
        Stream<Double> stream = Stream.of(500.0, 1000.0);
        assertThat(stream.parallel().collect(new SummingCollector())).isEqualTo(1500.0);
    }

    @Test
    void testValue3() {
        Stream<Double> stream = Stream.of(300.0, 500.0, 1000.0);
        assertThat(stream.parallel().collect(new SummingCollector())).isEqualTo(1800.0);
    }

    @Test
    void testValueN() {
        Stream<Double> stream = IntStream.range(300, 1000).mapToObj(a -> (double) a);
        assertThat(stream.parallel().collect(new SummingCollector())).isEqualTo(454650.0);
    }

}
