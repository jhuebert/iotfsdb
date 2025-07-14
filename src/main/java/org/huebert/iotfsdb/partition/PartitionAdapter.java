package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public interface PartitionAdapter {

    int getTypeSize();

    void put(ByteBuffer buffer, int index, Number value);

    Stream<Number> getStream(ByteBuffer buffer, int index, int length);

}
