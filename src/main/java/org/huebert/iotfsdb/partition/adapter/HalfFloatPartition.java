package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class HalfFloatPartition implements PartitionAdapter {

    @Override
    public int getTypeSize() {
        return Short.BYTES;
    }

    public Float get(ByteBuffer byteBuffer, Integer byteOffset) {
        float result = Float.float16ToFloat(byteBuffer.getShort(byteOffset));
        return Float.isNaN(result) ? null : result;
    }

    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        byteBuffer.putShort(byteOffset, Float.floatToFloat16(value == null ? Float.NaN : value.floatValue()));
    }

}
