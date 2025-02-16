package org.huebert.iotfsdb.partition;

import lombok.Data;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Stream;

@Data
public class MappedPartition implements PartitionAdapter {

    private final PartitionAdapter innerAdapter;

    private final RangeMapper mapper;

    public MappedPartition(PartitionAdapter innerAdapter, double min, double max) {
        this.innerAdapter = innerAdapter;
        int bits = innerAdapter.getTypeSize() << 3;
        double halfRange = Math.pow(2, bits - 1);
        this.mapper = new RangeMapper(min, max, -halfRange + 1, halfRange - 1, true);
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
                return mapper.decode(innerValue.doubleValue());
            });
    }

    @Override
    public void put(ByteBuffer buffer, int index, Number value) {
        Double result = null;
        if (value != null) {
            result = Math.rint(mapper.encode(value.doubleValue()));
        }
        innerAdapter.put(buffer, index, result);
    }

}
