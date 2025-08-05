package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class BytePartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Byte.BYTES;
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(
            buffer.slice(index, length),
            length,
            b -> NumberConverter.fromByte(b.get())
        ).asStream();
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.put(index, NumberConverter.toByte(value));
    }

}
