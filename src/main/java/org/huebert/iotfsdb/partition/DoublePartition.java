package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class DoublePartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Double.BYTES;
    }

    @Override
    public Iterator<Number> getIterator(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(buffer.asDoubleBuffer().slice(index, length), b -> NumberConverter.fromDouble(b.get()));
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asDoubleBuffer().put(index, NumberConverter.toDouble(value));
    }

}
