package org.huebert.iotfsdb.partition;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Stream;

public class MappedPartition implements PartitionAdapter {

    @Getter
    private final PartitionAdapter innerAdapter;

    @Getter
    private final double min;

    @Getter
    private final double max;

    private final double getRangeRatio;

    private final double putRangeRatio;

    private final double mappedMin;

    public MappedPartition(PartitionAdapter innerAdapter, double min, double max) {
        this.innerAdapter = innerAdapter;
        this.min = min;
        this.max = max;

        int bits = innerAdapter.getTypeSize() << 3;
        this.mappedMin = -Math.pow(2, bits - 1) + 1;

        double range = max - min;
        double mappedRange = Math.pow(2, bits) - 2;
        this.putRangeRatio = mappedRange / range;
        this.getRangeRatio = range / mappedRange;
    }

    @Override
    public int getTypeSize() {
        return innerAdapter.getTypeSize();
    }

    @Override
    public Iterator<Number> getIterator(ByteBuffer buffer, int index, int length) {
        return innerAdapter.getIterator(buffer, index, length);
    }

    @Override
    public Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        return PartitionAdapter.super.getStream(buffer, index, length)
            .map(innerValue -> {
                Number result = innerValue;
                if (innerValue != null) {
                    result = map(innerValue.doubleValue(), mappedMin, min, getRangeRatio);
                }
                return result;
            });
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        Double result = null;
        if (value != null) {
            result = Math.rint(map(constrain(value.doubleValue()), min, mappedMin, putRangeRatio));
        }
        innerAdapter.put(buffer, index, result);
    }

    private double map(double value, double inMin, double outMin, double rangeRatio) {
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
