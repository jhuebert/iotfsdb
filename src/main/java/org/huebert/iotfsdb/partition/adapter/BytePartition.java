package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class BytePartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Byte.BYTES;
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        byte result = byteBuffer.get(byteOffset);
        return result == Byte.MIN_VALUE ? null : result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        byteBuffer.put(byteOffset, value == null ? Byte.MIN_VALUE : value.byteValue());
    }
}
