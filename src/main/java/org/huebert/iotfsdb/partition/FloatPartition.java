package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class FloatPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Float.BYTES;
    }

    @Override
    public Iterator<Number> getIterator(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(buffer.asFloatBuffer().slice(index, length), b -> NumberConverter.fromFloat(b.get()));
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asFloatBuffer().put(index, NumberConverter.toFloat(value));
    }

}
