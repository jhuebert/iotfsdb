package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class FloatPartition extends Partition<Float> {

    public FloatPartition(Path path, LocalDateTime start, Period period, Duration interval) {
        super(path, start, period, interval, Float.BYTES, FloatPartition::getValue, FloatPartition::putValue);
    }

    private static Float getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        float result = byteBuffer.getFloat(byteOffset);
        return Float.isNaN(result) ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        return byteBuffer.putFloat(byteOffset, value == null ? Float.NaN : value.floatValue());
    }

}
