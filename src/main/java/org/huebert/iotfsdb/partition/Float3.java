package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;

/**
 * Utility class for encoding/decoding double precision values to/from a 24-bit (3-byte)
 * floating point format.
 * <p>
 * Format specification:
 * - 1 bit for sign
 * - 7 bits for exponent
 * - 16 bits for mantissa
 */
public final class Float3 extends Number {

    public static final int BYTES = 3;

    public static final Float3 NaN = new Float3(new byte[] {0x7F, (byte) 0xFF, (byte) 0xFF});

    public static final Float3 ZERO = new Float3(new byte[] {0, 0, 0});

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

    /**
     * Encodes a double precision value into a 24-bit (3-byte) floating point format.
     *
     * @param value The double value to encode
     * @return A byte array of length 3 containing the encoded value
     */
    public static Float3 fromDouble(double value) {

        if (Double.isNaN(value)) {
            return NaN;
        }

        if (Double.isInfinite(value)) {
            return (value > 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        if (value == 0.0) {
            return ZERO;
        }

        long bits = Double.doubleToLongBits(value);

        int exponent = (int) ((bits >>> 52) & 0x7FF) - DOUBLE_EXPONENT_ADJUSTMENT;
        if (exponent <= 0) {
            return ZERO;
        }

        byte sign = (byte) ((bits >>> 56) & 0x80);
        if (exponent >= 127) {
            return (sign == 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        long mantissa = bits & 0x000FFFFFFFFFFFFFL;

        return new Float3(new byte[] {
            (byte) (sign | exponent),
            (byte) (mantissa >>> 44),
            (byte) (mantissa >>> 36)
        });
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    /**
     * Checks if the encoded value is Not-a-Number (NaN).
     *
     * @return true if the encoded value is NaN, false otherwise
     * @throws IllegalArgumentException if the input array is not exactly 3 bytes
     */
    public boolean isNaN() {
        // NaN has exponent all 1s and non-zero mantissa
        int exponent = (bytes[0] & 0x7F);
        return exponent == 0x7F && ((bytes[1] & 0xFF) != 0 || (bytes[2] & 0xFF) != 0);
    }

    /**
     * Checks if the encoded value is infinite (either positive or negative infinity).
     *
     * @return true if the encoded value is infinite, false otherwise
     * @throws IllegalArgumentException if the input array is not exactly 3 bytes
     */
    public boolean isInfinite() {
        // Infinity has exponent all 1s and zero mantissa
        int exponent = (bytes[0] & 0x7F);
        return exponent == 0x7F && bytes[1] == 0 && bytes[2] == 0;
    }

    /**
     * Checks if the encoded value is positive infinity.
     *
     * @return true if the encoded value is positive infinity, false otherwise
     * @throws IllegalArgumentException if the input array is not exactly 3 bytes
     */
    public boolean isPositiveInfinity() {
        // Positive infinity has sign bit 0, exponent all 1s and zero mantissa
        return bytes[0] == 0x7F && bytes[1] == 0 && bytes[2] == 0;
    }

    /**
     * Checks if the encoded value is negative infinity.
     *
     * @return true if the encoded value is negative infinity, false otherwise
     * @throws IllegalArgumentException if the input array is not exactly 3 bytes
     */
    public boolean isNegativeInfinity() {
        // Negative infinity has sign bit 1, exponent all 1s and zero mantissa
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
        int mantissa = ((bytes[1] << 8) & 0xFF00) | bytes[2];

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
        int mantissa = ((bytes[1] << 8) & 0xFF00) | bytes[2];

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
