package org.huebert.iotfsdb.collectors;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class BigDecimalSummingCollectorTest {

    @Test
    void testEmpty() {
        Stream<BigDecimal> stream = Stream.empty();
        assertThat(stream.parallel().collect(new BigDecimalSummingCollector())).isEqualTo(BigDecimal.ZERO);
    }

    @Test
    void testValue1() {
        Stream<BigDecimal> stream = Stream.of(new BigDecimal("1000"));
        assertThat(stream.parallel().collect(new BigDecimalSummingCollector())).isEqualTo(new BigDecimal("1000"));
    }

    @Test
    void testValue2() {
        Stream<BigDecimal> stream = Stream.of(new BigDecimal("500"), new BigDecimal("1000"));
        assertThat(stream.parallel().collect(new BigDecimalSummingCollector())).isEqualTo(new BigDecimal("1500"));
    }

    @Test
    void testValue3() {
        Stream<BigDecimal> stream = Stream.of(new BigDecimal("300"), new BigDecimal("500"), new BigDecimal("1000"));
        assertThat(stream.parallel().collect(new BigDecimalSummingCollector())).isEqualTo(new BigDecimal("1800"));
    }

    @Test
    void testValueN() {
        Stream<BigDecimal> stream = IntStream.range(300, 1000).mapToObj(BigDecimal::new);
        assertThat(stream.parallel().collect(new BigDecimalSummingCollector())).isEqualTo(new BigDecimal("454650"));
    }

}