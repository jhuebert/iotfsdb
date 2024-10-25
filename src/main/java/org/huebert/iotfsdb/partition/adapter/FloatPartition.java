package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class FloatPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Float.BYTES;
    }

    public Float get(ByteBuffer byteBuffer, Integer byteOffset) {
        float result = byteBuffer.getFloat(byteOffset);
        return Float.isNaN(result) ? null : result;
    }

    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        byteBuffer.putFloat(byteOffset, value == null ? Float.NaN : value.floatValue());
    }

}
