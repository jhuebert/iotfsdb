package org.huebert.iotfsdb.partition;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Stream;

public class CurvedMappedPartition implements PartitionAdapter {

    private static final double NOMINAL_MIN_RATIO = -1.0;

    @Getter
    private final PartitionAdapter innerAdapter;

    @Getter
    private final double min;

    @Getter
    private final double max;

    private final double getRatio;

    private final double putRatio;

    private final double encodedRange;

    public CurvedMappedPartition(PartitionAdapter innerAdapter, double min, double max) {
        this.innerAdapter = innerAdapter;
        this.min = min;
        this.max = max;

        double range = max - min;
        this.putRatio = 2.0 / range;
        this.getRatio = range / 2.0;

        int bits = innerAdapter.getTypeSize() << 3;
        encodedRange = Math.pow(2, bits - 1) - 1;
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
                    double ratio = decurve(innerValue.doubleValue() / encodedRange);
                    result = map(ratio, NOMINAL_MIN_RATIO, min, getRatio);
                }
                return result;
            });
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        Double result = null;
        if (value != null) {
            double ratio = map(value.doubleValue(), min, NOMINAL_MIN_RATIO, putRatio);
            result = Math.rint(curve(ratio) * encodedRange);
        }
        innerAdapter.put(buffer, index, result);
    }

    private double curve(double value) {
        double e = Math.exp(-2.0 * value);
        return (2.0 / (1.0 + e)) - 1.0;
    }

    private double decurve(double value) {
        double a = (1.0 + value) / (1.0 - value);
        return 0.5 * Math.log(a);
    }

    private double map(double value, double inMin, double outMin, double rangeRatio) {
        return ((value - inMin) * rangeRatio) + outMin;
    }

}
