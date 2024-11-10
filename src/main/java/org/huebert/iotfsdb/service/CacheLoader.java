package org.huebert.iotfsdb.service;

import java.util.function.Function;

public class CacheLoader<K, V> extends com.google.common.cache.CacheLoader<K, V> {

    private final Function<K, V> function;

    public CacheLoader(Function<K, V> function) {
        this.function = function;
    }

    @Override
    public V load(K key) {
        return function.apply(key);
    }

}
