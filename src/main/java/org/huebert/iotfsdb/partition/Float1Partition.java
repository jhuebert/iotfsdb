package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class Float1Partition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Float1.BYTES;
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(
            buffer.slice(index * Float1.BYTES, length * Float1.BYTES),
            length,
            this::readFloat1
        ).asStream();
    }

    private Number readFloat1(ByteBuffer buffer) {
        Float1 value = Float1.readFrom(buffer);
        return value.isNaN() ? null : value;
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        Float1 float1 = value == null ? Float1.NaN : Float1.fromDouble(value.doubleValue());
        float1.writeTo(buffer.slice(index * Float1.BYTES, Float1.BYTES));
    }

}
