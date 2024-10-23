package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class IntegerPartition extends Partition<Integer> {

    protected IntegerPartition(Path path, LocalDateTime start, Period period, Duration interval) {
        super(path, start, period, interval, Integer.BYTES, IntegerPartition::getValue, IntegerPartition::putValue);
    }

    private static Integer getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        int result = byteBuffer.getInt(byteOffset);
        return result == Integer.MIN_VALUE ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        return byteBuffer.putInt(byteOffset, value == null ? Integer.MIN_VALUE : value.intValue());
    }

}
