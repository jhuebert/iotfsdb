package org.huebert.iotfsdb.util;

public record Tuple<K, V>(K key, V value) {
}
