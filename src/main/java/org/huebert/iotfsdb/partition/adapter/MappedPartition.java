package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class MappedPartition implements PartitionAdapter {

    private final PartitionAdapter inner;

    private final double min;

    private final double max;

    private final double range;

    private final double mappedRange;

    private final double mappedMin;

    public MappedPartition(PartitionAdapter inner, double min, double max) {
        this.inner = inner;
        this.min = min;
        this.max = max;
        this.range = max - min;

        int bits = inner.getTypeSize() << 3;
        this.mappedRange = Math.pow(2, bits) - 2;
        this.mappedMin = -Math.pow(2, bits - 1) + 1;
    }

    @Override
    public int getTypeSize() {
        return inner.getTypeSize();
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        Number result = inner.get(byteBuffer, byteOffset);
        if (result != null) {
            result = map(result.doubleValue(), mappedMin, mappedRange, min, range);
        }
        return result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        Double result = null;
        if (value != null) {
            result = map(constrain(value.doubleValue()), min, range, mappedMin, mappedRange);
        }
        inner.put(byteBuffer, byteOffset, result);
    }

    double map(double value, double inMin, double inRange, double outMin, double outRange) {
        return (((value - inMin) * outRange) / inRange) + outMin;
    }

    private double constrain(double value) {
        if (value < min) {
            return min;
        } else if (value > max) {
            return max;
        }
        return value;
    }

}
