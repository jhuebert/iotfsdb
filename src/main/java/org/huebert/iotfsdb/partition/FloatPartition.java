package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class FloatPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Float.BYTES;
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(
            buffer.asFloatBuffer().slice(index, length),
            length,
            b -> NumberConverter.fromFloat(b.get())
        ).asStream();
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asFloatBuffer().put(index, NumberConverter.toFloat(value));
    }

}
