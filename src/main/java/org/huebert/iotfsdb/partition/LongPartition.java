package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class LongPartition extends Partition<Long> {

    protected LongPartition(Path path, LocalDateTime start, Period period, Duration interval) {
        super(path, start, period, interval, Long.BYTES, LongPartition::getValue, LongPartition::putValue);
    }

    private static Long getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        long result = byteBuffer.getLong(byteOffset);
        return result == Long.MIN_VALUE ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        return byteBuffer.putLong(byteOffset, value == null ? Long.MIN_VALUE : value.longValue());
    }

}
