package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class DoublePartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Double.BYTES;
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        double result = byteBuffer.getDouble(byteOffset);
        return Double.isNaN(result) ? null : result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        byteBuffer.putDouble(byteOffset, value == null ? Double.NaN : value.doubleValue());
    }
}
