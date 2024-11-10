package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class LongPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Long.BYTES;
    }

    @Override
    public Iterator<Number> getIterator(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(buffer.asLongBuffer().slice(index, length), b -> NumberConverter.fromLong(b.get()));
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.asLongBuffer().put(index, NumberConverter.toLong(value));
    }

}
