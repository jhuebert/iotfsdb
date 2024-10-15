package org.huebert.iotfsdb.partition;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

public class DoublePartition extends Partition<Double> {

    protected DoublePartition(File file, LocalDateTime start, Period period, Duration interval) {
        super(file, start, period, interval, Double.BYTES, DoublePartition::getValue, DoublePartition::putValue, Double::parseDouble);
    }

    private static Double getValue(ByteBuffer byteBuffer, Integer byteOffset) {
        double result = byteBuffer.getDouble(byteOffset);
        return Double.isNaN(result) ? null : result;
    }

    private static ByteBuffer putValue(ByteBuffer byteBuffer, Integer byteOffset, Double value) {
        return byteBuffer.putDouble(byteOffset, value == null ? Double.NaN : value);
    }

}
