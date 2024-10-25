package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class ShortPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Short.BYTES;
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        short result = byteBuffer.getShort(byteOffset);
        return result == Short.MIN_VALUE ? null : result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        byteBuffer.putShort(byteOffset, value == null ? Short.MIN_VALUE : value.shortValue());
    }
}
