package org.huebert.iotfsdb.partition;

public class NumberConverter {

    public static Number fromByte(byte value) {
        return value == Byte.MIN_VALUE ? null : value;
    }

    public static byte toByte(Number value) {
        return value == null ? Byte.MIN_VALUE : value.byteValue();
    }

    public static Number fromDouble(double value) {
        return Double.isNaN(value) ? null : value;
    }

    public static double toDouble(Number value) {
        return value == null ? Double.NaN : value.doubleValue();
    }

    public static Number fromFloat(float value) {
        return Float.isNaN(value) ? null : value;
    }

    public static float toFloat(Number value) {
        return value == null ? Float.NaN : value.floatValue();
    }

    public static Number fromHalfFloat(short value) {
        return fromFloat(Float.float16ToFloat(value));
    }

    public static short toHalfFloat(Number value) {
        return Float.floatToFloat16(value == null ? Float.NaN : value.floatValue());
    }

    public static Number fromInt(int value) {
        return value == Integer.MIN_VALUE ? null : value;
    }

    public static int toInt(Number value) {
        return value == null ? Integer.MIN_VALUE : value.intValue();
    }

    public static Number fromLong(long value) {
        return value == Long.MIN_VALUE ? null : value;
    }

    public static long toLong(Number value) {
        return value == null ? Long.MIN_VALUE : value.longValue();
    }

    public static Number fromShort(short value) {
        return value == Short.MIN_VALUE ? null : value;
    }

    public static short toShort(Number value) {
        return value == null ? Short.MIN_VALUE : value.shortValue();
    }

}
