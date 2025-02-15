package org.huebert.iotfsdb.partition;

import lombok.Data;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Stream;

@Data
public class CurvedMappedPartition implements PartitionAdapter {

    private final PartitionAdapter innerAdapter;

    private final RangeMapper mapper;

    private final double encodedRange;

    public CurvedMappedPartition(PartitionAdapter innerAdapter, double min, double max) {
        this.innerAdapter = innerAdapter;
        this.mapper = new RangeMapper(min, max, -1.0, 1.0, false);
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
                if (innerValue == null) {
                    return null;
                }
                return mapper.decode(decurve(innerValue.doubleValue() / encodedRange));
            });
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        Double result = null;
        if (value != null) {
            double ratio = mapper.encode(value.doubleValue());
            result = Math.rint(curve(ratio) * encodedRange);
        }
        innerAdapter.put(buffer, index, result);
    }

    private static double curve(double value) {
        double e = Math.exp(-2.0 * value);
        return (1.0 - e) / (1.0 + e);
    }

    private static double decurve(double value) {
        double a = (1.0 + value) / (1.0 - value);
        return 0.5 * Math.log(a);
    }

}
