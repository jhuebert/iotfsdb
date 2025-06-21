package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Format specification:
 * - 1 bit for sign
 * - 7 bits for exponent
 * - 16 bits for mantissa
 */
public final class Float3 extends Number {

    public static final int BYTES = 3;

    public static final Float3 NaN = new Float3(new byte[] {0x7F, (byte) 0xFF, (byte) 0xFF});

    public static final Float3 ZERO = new Float3(new byte[] {0, 0, 0});

    public static final Float3 NEGATIVE_ZERO = new Float3(new byte[] {(byte) 0x80, 0, 0});

    public static final Float3 POSITIVE_INFINITY = new Float3(new byte[] {0x7F, 0, 0});

    public static final Float3 NEGATIVE_INFINITY = new Float3(new byte[] {(byte) 0xFF, 0, 0});

    private static final int DOUBLE_BIAS = 1023;

    private static final int FLOAT_BIAS = 127;

    private static final int FLOAT3_BIAS = 63;

    private static final int DOUBLE_EXPONENT_ADJUSTMENT = DOUBLE_BIAS - FLOAT3_BIAS;

    private static final int FLOAT_EXPONENT_ADJUSTMENT = FLOAT_BIAS - FLOAT3_BIAS;

    private final byte[] bytes;

    private Float3(byte[] bytes) {
        this.bytes = bytes;
    }

    public static Float3 readFrom(ByteBuffer buffer) {
        byte[] bytes = new byte[BYTES];
        buffer.get(bytes);
        return new Float3(bytes);
    }

    public static Float3 fromFloat(float value) {

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

        if (exponent >= 127) {
            return (sign == 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        int mantissa = bits & 0x7FFFFF;

        return new Float3(new byte[] {
            (byte) (sign | exponent),
            (byte) (mantissa >>> 15),
            (byte) (mantissa >>> 7)
        });
    }

    public static Float3 fromDouble(double value) {

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

        if (exponent >= 127) {
            return (sign == 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        long mantissa = bits & 0xFFFFFFFFFFFFFL;

        return new Float3(new byte[] {
            (byte) (sign | exponent),
            (byte) (mantissa >>> 44),
            (byte) (mantissa >>> 36)
        });
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Float3 float3 = (Float3) o;
        return Objects.deepEquals(bytes, float3.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "Float3{" +
            "bytes=" + Arrays.toString(bytes) +
            '}';
    }

    public boolean isNaN() {
        int exponent = (bytes[0] & 0x7F);
        return exponent == 0x7F && ((bytes[1] & 0xFF) != 0 || (bytes[2] & 0xFF) != 0);
    }

    public boolean isInfinite() {
        int exponent = (bytes[0] & 0x7F);
        return exponent == 0x7F && bytes[1] == 0 && bytes[2] == 0;
    }

    public boolean isPositiveInfinity() {
        return bytes[0] == 0x7F && bytes[1] == 0 && bytes[2] == 0;
    }

    public boolean isNegativeInfinity() {
        return bytes[0] == (byte) 0xFF && bytes[1] == 0 && bytes[2] == 0;
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

        int sign = bytes[0] & 0x80;
        int exponent = bytes[0] & 0x7F;
        int mantissa = ((bytes[1] << 8) & 0xFF00) | bytes[2] & 0xFF;

        if (exponent == 0 && mantissa == 0) {
            return (sign == 0) ? 0.0f : -0.0f;
        }

        if (exponent == 0x7F) {
            return mantissa == 0
                ? (sign == 0 ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY)
                : Float.NaN;
        }

        return Float.intBitsToFloat(sign << 24 | (exponent + FLOAT_EXPONENT_ADJUSTMENT) << 23 | mantissa << 7);
    }

    @Override
    public double doubleValue() {

        int sign = bytes[0] & 0x80;
        int exponent = bytes[0] & 0x7F;
        int mantissa = ((bytes[1] << 8) & 0xFF00) | bytes[2] & 0xFF;

        if (exponent == 0 && mantissa == 0) {
            return (sign == 0) ? 0.0 : -0.0;
        }

        if (exponent == 0x7F) {
            return mantissa == 0
                ? (sign == 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY)
                : Double.NaN;
        }

        return Double.longBitsToDouble((long) sign << 56 | (long) (exponent + DOUBLE_EXPONENT_ADJUSTMENT) << 52 | (long) mantissa << 36);
    }

}
