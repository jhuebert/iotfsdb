package org.huebert.iotfsdb.partition;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class LongPartition extends Partition<Long> {

    protected LongPartition(File file, LocalDateTime start, Period period, Duration interval) {
        super(file, start, period, interval, Long.BYTES, LongPartition::getValue, LongPartition::putValue, Long::parseLong);
    }

    private static Long getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        long result = byteBuffer.getLong(byteOffset);
        return result == Long.MIN_VALUE ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Long value) {
        return byteBuffer.putLong(byteOffset, value == null ? Long.MIN_VALUE : value);
    }

}
