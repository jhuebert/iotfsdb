package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class LongPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Long.BYTES;
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        long result = byteBuffer.getLong(byteOffset);
        return result == Long.MIN_VALUE ? null : result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        byteBuffer.putLong(byteOffset, value == null ? Long.MIN_VALUE : value.longValue());
    }
}
