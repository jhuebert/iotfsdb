package org.huebert.iotfsdb.collectors;

import java.util.stream.Collector;

public interface NumberCollector<I, A> extends Collector<I, A, Number> {

}
