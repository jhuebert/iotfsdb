package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class LongPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Long.BYTES;
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(
            buffer.asLongBuffer().slice(index, length),
            length,
            b -> NumberConverter.fromLong(b.get())
        ).asStream();
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asLongBuffer().put(index, NumberConverter.toLong(value));
    }

}
