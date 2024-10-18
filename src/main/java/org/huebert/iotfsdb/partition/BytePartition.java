package org.huebert.iotfsdb.partition;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class BytePartition extends Partition<Byte> {

    protected BytePartition(File file, LocalDateTime start, Period period, Duration interval) {
        super(file, start, period, interval, Byte.BYTES, BytePartition::getValue, BytePartition::putValue);
    }

    private static Byte getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        byte result = byteBuffer.get(byteOffset);
        return result == Byte.MIN_VALUE ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        return byteBuffer.put(byteOffset, value == null ? Byte.MIN_VALUE : value.byteValue());
    }

}
