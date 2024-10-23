package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class ShortPartition extends Partition<Short> {

    protected ShortPartition(Path path, LocalDateTime start, Period period, Duration interval) {
        super(path, start, period, interval, Short.BYTES, ShortPartition::getValue, ShortPartition::putValue);
    }

    private static Short getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        short result = byteBuffer.getShort(byteOffset);
        return result == Short.MIN_VALUE ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        return byteBuffer.putShort(byteOffset, value == null ? Short.MIN_VALUE : value.shortValue());
    }

}
