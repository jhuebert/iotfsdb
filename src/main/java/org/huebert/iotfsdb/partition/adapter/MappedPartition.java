package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class MappedPartition implements PartitionAdapter {

    private final PartitionAdapter inner;

    private final double min;

    private final double max;

    private final double getRangeRatio;

    private final double putRangeRatio;

    private final double mappedMin;

    public MappedPartition(PartitionAdapter inner, double min, double max) {
        this.inner = inner;
        this.min = min;
        this.max = max;

        int bits = inner.getTypeSize() << 3;
        this.mappedMin = -Math.pow(2, bits - 1) + 1;

        double range = max - min;
        double mappedRange = Math.pow(2, bits) - 2;
        this.putRangeRatio = mappedRange / range;
        this.getRangeRatio = range / mappedRange;
    }

    @Override
    public int getTypeSize() {
        return inner.getTypeSize();
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        Number result = inner.get(byteBuffer, byteOffset);
        if (result != null) {
            result = map(result.doubleValue(), mappedMin, min, getRangeRatio);
        }
        return result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        Double result = null;
        if (value != null) {
            result = map(constrain(value.doubleValue()), min, mappedMin, putRangeRatio);
        }
        inner.put(byteBuffer, byteOffset, result);
    }

    double map(double value, double inMin, double outMin, double rangeRatio) {
        return ((value - inMin) * rangeRatio) + outMin;
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
