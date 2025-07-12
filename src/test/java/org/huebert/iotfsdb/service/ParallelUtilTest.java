package org.huebert.iotfsdb.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelUtilTest {

    @Test
    public void testForEach() {
        AtomicInteger value = new AtomicInteger(0);
        ParallelUtil.forEach(List.of(1, 2, 3), value::addAndGet);
        assertThat(value.get()).isEqualTo(6);
    }

    @Test
    public void testForEachException() {
        assertThrows(RuntimeException.class, () -> ParallelUtil.forEach(List.of(1, 2, 3), a -> {
            throw new IllegalArgumentException();
        }));
    }

    @Test
    public void testMap() {
        List<Integer> result = ParallelUtil.map(List.of(1, 2, 3), a -> a + 10);
        assertThat(result).isEqualTo(List.of(11, 12, 13));
    }

    @Test
    public void testMapException() {
        assertThrows(RuntimeException.class, () -> ParallelUtil.map(List.of(1, 2, 3), a -> {
            throw new IllegalArgumentException();
        }));
    }

}
