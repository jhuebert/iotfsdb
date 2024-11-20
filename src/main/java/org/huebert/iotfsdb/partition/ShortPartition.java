package org.huebert.iotfsdb.partition;


import java.nio.ByteBuffer;
import java.util.Iterator;

public class ShortPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Short.BYTES;
    }

    @Override
    public Iterator<Number> getIterator(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(buffer.asShortBuffer().slice(index, length), b -> NumberConverter.fromShort(b.get()));
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asShortBuffer().put(index, NumberConverter.toShort(value));
    }

}
