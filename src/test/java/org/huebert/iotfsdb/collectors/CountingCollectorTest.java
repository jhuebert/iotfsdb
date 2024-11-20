package org.huebert.iotfsdb.collectors;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class CountingCollectorTest {

    @Test
    void testEmpty() {
        Stream<Double> stream = Stream.empty();
        assertThat(stream.parallel().collect(new CountingCollector())).isEqualTo(0L);
    }

    @Test
    void testValue1() {
        Stream<Double> stream = Stream.of(1000.0);
        assertThat(stream.parallel().collect(new CountingCollector())).isEqualTo(1L);
    }

    @Test
    void testValue2() {
        Stream<Double> stream = Stream.of(500.0, 1000.0);
        assertThat(stream.parallel().collect(new CountingCollector())).isEqualTo(2L);
    }

    @Test
    void testValue3() {
        Stream<Double> stream = Stream.of(300.0, 500.0, 1000.0);
        assertThat(stream.parallel().collect(new CountingCollector())).isEqualTo(3L);
    }

    @Test
    void testValueN() {
        Stream<Double> stream = IntStream.range(300, 1000).mapToObj(a -> (double) a);
        assertThat(stream.parallel().collect(new CountingCollector())).isEqualTo(700L);
    }

}
