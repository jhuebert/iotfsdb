package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class IntegerPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Integer.BYTES;
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        int result = byteBuffer.getInt(byteOffset);
        return result == Integer.MIN_VALUE ? null : result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        byteBuffer.putInt(byteOffset, value == null ? Integer.MIN_VALUE : value.intValue());
    }
}
