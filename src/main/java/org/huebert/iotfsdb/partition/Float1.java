package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Format specification:
 * - 1 bit for sign
 * - 4 bits for exponent
 * - 3 bits for mantissa
 */
public final class Float1 extends Number {

    public static final int BYTES = 1;

    public static final Float1 NaN = new Float1((byte) 0x7F);

    public static final Float1 ZERO = new Float1((byte) 0x00);

    public static final Float1 NEGATIVE_ZERO = new Float1((byte) 0x80);

    public static final Float1 POSITIVE_INFINITY = new Float1((byte) 0x78);

    public static final Float1 NEGATIVE_INFINITY = new Float1((byte) 0xF8);

    private static final int DOUBLE_BIAS = 1023;

    private static final int FLOAT_BIAS = 127;

    private static final int FLOAT1_BIAS = 7;

    private static final int DOUBLE_EXPONENT_ADJUSTMENT = DOUBLE_BIAS - FLOAT1_BIAS;

    private static final int FLOAT_EXPONENT_ADJUSTMENT = FLOAT_BIAS - FLOAT1_BIAS;

    private final byte value;

    private Float1(byte value) {
        this.value = value;
    }

    public static Float1 readFrom(ByteBuffer buffer) {
        return new Float1(buffer.get());
    }

    public static Float1 fromFloat(float value) {

        if (Float.isNaN(value)) {
            return NaN;
        }

        if (Float.isInfinite(value)) {
            return (value > 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        int bits = Float.floatToIntBits(value);

        byte sign = (byte) ((bits >>> 24) & 0x80);
        if (value == 0.0f) {
            return (sign == 0) ? ZERO : NEGATIVE_ZERO;
        }

        int exponent = ((bits >>> 23) & 0xFF) - FLOAT_EXPONENT_ADJUSTMENT;
        if (exponent < 0) {
            return (sign == 0) ? ZERO : NEGATIVE_ZERO;
        }

        if (exponent >= 15) {
            return (sign == 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        int mantissa = (bits >>> 20) & 0x7;

        return new Float1((byte) (sign | (exponent << 3) | mantissa));
    }

    public static Float1 fromDouble(double value) {

        if (Double.isNaN(value)) {
            return NaN;
        }

        if (Double.isInfinite(value)) {
            return (value > 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        long bits = Double.doubleToLongBits(value);

        byte sign = (byte) ((bits >>> 56) & 0x80);
        if (value == 0.0) {
            return (sign == 0) ? ZERO : NEGATIVE_ZERO;
        }

        int exponent = (int) ((bits >>> 52) & 0x7FF) - DOUBLE_EXPONENT_ADJUSTMENT;
        if (exponent < 0) {
            return (sign == 0) ? ZERO : NEGATIVE_ZERO;
        }

        if (exponent >= 15) {
            return (sign == 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        long mantissa = (bits >>> 49) & 0x7;

        return new Float1((byte) (sign | (exponent << 3) | mantissa));
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.put(value);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Float1 float1 = (Float1) o;
        return value == float1.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "Float1{" +
            "value=" + value +
            '}';
    }

    public boolean isNaN() {
        int exponent = (value & 0x78) >>> 3;
        int mantissa = value & 0x07;
        return exponent == 0x0F && mantissa != 0;
    }

    public boolean isInfinite() {
        int exponent = (value & 0x78) >>> 3;
        int mantissa = value & 0x07;
        return exponent == 0x0F && mantissa == 0;
    }

    public boolean isPositiveInfinity() {
        return value == 0x78;
    }

    public boolean isNegativeInfinity() {
        return value == (byte) 0xF8;
    }

    @Override
    public int intValue() {
        return (int) floatValue();
    }

    @Override
    public long longValue() {
        return (long) floatValue();
    }

    @Override
    public float floatValue() {

        int sign = value & 0x80;
        int exponent = (value & 0x78) >>> 3;
        int mantissa = value & 0x07;

        if (exponent == 0 && mantissa == 0) {
            return (sign == 0) ? 0.0f : -0.0f;
        }

        if (exponent == 0x0F) {
            return mantissa == 0
                ? (sign == 0 ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY)
                : Float.NaN;
        }

        return Float.intBitsToFloat(sign << 24 | (exponent + FLOAT_EXPONENT_ADJUSTMENT) << 23 | mantissa << 20);
    }

    @Override
    public double doubleValue() {

        int sign = value & 0x80;
        int exponent = (value & 0x78) >>> 3;
        int mantissa = value & 0x07;

        if (exponent == 0 && mantissa == 0) {
            return (sign == 0) ? 0.0 : -0.0;
        }

        if (exponent == 0x0F) {
            return mantissa == 0
                ? (sign == 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY)
                : Double.NaN;
        }

        return Double.longBitsToDouble((long) sign << 56 | (long) (exponent + DOUBLE_EXPONENT_ADJUSTMENT) << 52 | (long) mantissa << 49);
    }

}
