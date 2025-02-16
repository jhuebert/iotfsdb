package org.huebert.iotfsdb.collectors;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class BigDecimalMultiplyingCollectorTest {

    @Test
    void testEmpty() {
        Stream<BigDecimal> stream = Stream.empty();
        assertThat(stream.parallel().collect(new BigDecimalMultiplyingCollector())).isEqualTo(null);
    }

    @Test
    void testValue1() {
        Stream<BigDecimal> stream = Stream.of(new BigDecimal("1000"));
        assertThat(stream.parallel().collect(new BigDecimalMultiplyingCollector())).isEqualTo(new BigDecimal("1000"));
    }

    @Test
    void testValue2() {
        Stream<BigDecimal> stream = Stream.of(new BigDecimal("500"), new BigDecimal("1000"));
        assertThat(stream.parallel().collect(new BigDecimalMultiplyingCollector())).isEqualTo(new BigDecimal("500000"));
    }

    @Test
    void testValue3() {
        Stream<BigDecimal> stream = Stream.of(new BigDecimal("300"), new BigDecimal("500"), new BigDecimal("1000"));
        assertThat(stream.parallel().collect(new BigDecimalMultiplyingCollector())).isEqualTo(new BigDecimal("150000000"));
    }

    @Test
    void testValueN() {
        Stream<BigDecimal> stream = IntStream.range(300, 400).mapToObj(BigDecimal::new);
        assertThat(stream.parallel().collect(new BigDecimalMultiplyingCollector())).isEqualTo(new BigDecimal("156917867452978963018964401473624336794929971188905176198491787493398971473164819907109484262078025679359307053717933765536331148791075125948217007256552239231658338586788803047958724314527879061259431233617429609218245433606799360000000000000000000000000"));
    }

}
