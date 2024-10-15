package org.huebert.iotfsdb.partition;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class IntegerPartition extends Partition<Integer> {

    protected IntegerPartition(File file, LocalDateTime start, Period period, Duration interval) {
        super(file, start, period, interval, Integer.BYTES, IntegerPartition::getValue, IntegerPartition::putValue, Integer::parseInt);
    }

    private static Integer getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        int result = byteBuffer.getInt(byteOffset);
        return result == Integer.MIN_VALUE ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Integer value) {
        return byteBuffer.putInt(byteOffset, value == null ? Integer.MIN_VALUE : value);
    }

}
