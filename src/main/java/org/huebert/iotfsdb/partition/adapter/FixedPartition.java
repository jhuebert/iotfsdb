package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class FixedPartition implements PartitionAdapter {

    private final PartitionAdapter inner;

    private final double min;

    private final double range;

    private final double numValues;

    private final double minEncoded;

    public FixedPartition(PartitionAdapter inner, double min, double max) {
        this.inner = inner;
        this.min = min;
        this.range = max - min;

        int bits = inner.getTypeSize() << 3;
        this.numValues = Math.pow(2, bits) - 2;
        this.minEncoded = -Math.pow(2, bits - 1) + 1;
    }

    @Override
    public int getTypeSize() {
        return inner.getTypeSize();
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        Number result = inner.get(byteBuffer, byteOffset);
        if (result != null) {
            double ratio = constrain((result.doubleValue() - minEncoded) / numValues);
            result = (ratio * range) + min;
        }
        return result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        Double result = null;
        if (value != null) {
            double ratio = constrain((value.doubleValue() - min) / range);
            result = Math.rint((ratio * numValues) + minEncoded);
        }
        inner.put(byteBuffer, byteOffset, result);
    }

    private double constrain(double value) {
        if (value < 0.0) {
            return 0.0;
        } else if (value > 1.0) {
            return 1.0;
        }
        return value;
    }
}
