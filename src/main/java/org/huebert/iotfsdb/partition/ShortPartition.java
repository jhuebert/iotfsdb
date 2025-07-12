package org.huebert.iotfsdb.partition;


import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class ShortPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Short.BYTES;
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(
            buffer.asShortBuffer().slice(index, length),
            length,
            b -> NumberConverter.fromShort(b.get())
        ).asStream();
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asShortBuffer().put(index, NumberConverter.toShort(value));
    }

}
