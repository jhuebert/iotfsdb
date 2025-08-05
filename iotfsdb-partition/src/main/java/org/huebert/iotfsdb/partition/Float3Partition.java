package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class Float3Partition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Float3.BYTES;
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(
            buffer.slice(index * Float3.BYTES, length * Float3.BYTES),
            length,
            this::readFloat3
        ).asStream();
    }

    private Number readFloat3(ByteBuffer buffer) {
        Float3 value = Float3.readFrom(buffer);
        return value.isNaN() ? null : value;
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        Float3 float3 = value == null ? Float3.NaN : Float3.fromDouble(value.doubleValue());
        float3.writeTo(buffer.slice(index * Float3.BYTES, Float3.BYTES));
    }

}
