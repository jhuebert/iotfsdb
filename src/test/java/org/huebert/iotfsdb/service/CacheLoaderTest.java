package org.huebert.iotfsdb.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class CacheLoaderTest {

    @Test
    public void testLoad() {
        CacheLoader<Integer, Integer> loader = new CacheLoader<>(a -> a * 2);
        assertThat(loader.load(5)).isEqualTo(10);
    }

}
