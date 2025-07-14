package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class DoublePartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Double.BYTES;
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(
            buffer.asDoubleBuffer().slice(index, length),
            length,
            b -> NumberConverter.fromDouble(b.get())
        ).asStream();
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asDoubleBuffer().put(index, NumberConverter.toDouble(value));
    }

}
