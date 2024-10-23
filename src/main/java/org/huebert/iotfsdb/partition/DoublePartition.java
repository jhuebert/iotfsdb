package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class DoublePartition extends Partition<Double> {

    protected DoublePartition(Path path, LocalDateTime start, Period period, Duration interval) {
        super(path, start, period, interval, Double.BYTES, DoublePartition::getValue, DoublePartition::putValue);
    }

    private static Double getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        double result = byteBuffer.getDouble(byteOffset);
        return Double.isNaN(result) ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Number value) {
        return byteBuffer.putDouble(byteOffset, value == null ? Double.NaN : value.doubleValue());
    }

}
