package org.huebert.iotfsdb.partition.adapter;

import java.nio.ByteBuffer;

public interface PartitionAdapter {

    int getTypeSize();

    Number get(ByteBuffer byteBuffer, Integer byteOffset);

    void put(ByteBuffer byteBuffer, Integer byteOffset, Number value);

    default int getBitShift() {
        return (int) Math.rint(Math.log(getTypeSize()) / Math.log(2));
    }

    default boolean isMultiple(long numBytes) {
        return numBytes % getTypeSize() == 0;
    }

}
