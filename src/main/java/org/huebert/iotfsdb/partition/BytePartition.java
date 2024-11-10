package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class BytePartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Byte.BYTES;
    }

    @Override
    public Iterator<Number> getIterator(ByteBuffer buffer, int index, int length) {
        return new BufferIterator<>(buffer.slice(index, length), b -> NumberConverter.fromByte(b.get()));
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        buffer.put(index, NumberConverter.toByte(value));
    }

}
