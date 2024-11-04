package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public class CurvedMappedPartition implements PartitionAdapter {

    private static final double NOMINAL_MIN_RATIO = -1.0;

    private static final double MIN_RATIO = -3.0;

    private static final double MAX_RATIO = 3.0;

    private final PartitionAdapter inner;

    private final double min;

    private final double getRatio;

    private final double putRatio;

    private final double encodedRange;

    public CurvedMappedPartition(PartitionAdapter inner, double min, double max) {
        this.inner = inner;
        this.min = min;

        double range = max - min;
        this.putRatio = 2.0 / range;
        this.getRatio = range / 2.0;

        int bits = inner.getTypeSize() << 3;
        encodedRange = Math.pow(2, bits - 1) - 1;
    }

    @Override
    public int getTypeSize() {
        return inner.getTypeSize();
    }

    @Override
    public Number get(ByteBuffer byteBuffer, Integer byteOffset) {
        Number result = inner.get(byteBuffer, byteOffset);
        if (result != null) {
            double ratio = decurve(result.doubleValue() / encodedRange);
            result = map(ratio, NOMINAL_MIN_RATIO, min, getRatio);
        }
        return result;
    }

    @Override
    public void put(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        Double result = null;
        if (value != null) {
            double ratio = map(value.doubleValue(), min, NOMINAL_MIN_RATIO, putRatio);
            result = Math.rint(curve(ratio) * encodedRange);
        }
        inner.put(byteBuffer, byteOffset, result);
    }

    double curve(double value) {
        double a = Math.min(Math.max(value, MIN_RATIO), MAX_RATIO);
        double b = Math.exp(-2.0 * a);
        return (2.0 / (1.0 + b)) - 1.0;
    }

    double decurve(double value) {
        double a = (1.0 + value) / (1.0 - value);
        return 0.5 * Math.log(a);
    }

    double map(double value, double inMin, double outMin, double rangeRatio) {
        return ((value - inMin) * rangeRatio) + outMin;
    }

}
