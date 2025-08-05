package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class IntegerPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Integer.BYTES;
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(
            buffer.asIntBuffer().slice(index, length),
            length,
            b -> NumberConverter.fromInt(b.get())
        ).asStream();
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asIntBuffer().put(index, NumberConverter.toInt(value));
    }


}
