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

    private static final int FLOAT3_BIAS = 63;

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

        // Get the binary representation of the double
        long bits = Double.doubleToLongBits(Math.abs(value));

        // Extract the exponent from the double (11 bits in IEEE 754)
        int exponent = (int) ((bits >>> 52) & 0x7FF);

        // Adjust the exponent for our 7-bit format
        // IEEE 754 double has a bias of 1023, we'll use a bias of 63 for our 7-bit exponent
        exponent = exponent - DOUBLE_BIAS + FLOAT3_BIAS;

        // Handle very small numbers and underflow
        if (exponent <= 0) {
            // Subnormal numbers or underflow
            return ZERO;
        }

        // Handle overflow
        if (exponent >= 127) {
            // Return infinity with the appropriate sign
            return (value > 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        // Extract the mantissa from the double (52 bits in IEEE 754)
        long mantissa = bits & 0xFFFFFFFFFFFFFL;

        // Adjust the mantissa for our 16-bit format by shifting
        // IEEE 754 double has 52 bits, we need 16 bits
        mantissa = mantissa >>> (52 - 16);

        // Extract the sign bit (1 for negative, 0 for positive)
        int sign = (value < 0) ? 1 : 0;

        // Combine sign, exponent and mantissa into 24 bits
        int result = (sign << 23) | (exponent << 16) | (int) (mantissa & 0xFFFF);

        // Convert to 3 bytes
        return new Float3(new byte[] {
            (byte) ((result >>> 16) & 0xFF),
            (byte) ((result >>> 8) & 0xFF),
            (byte) (result & 0xFF)
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
        return (int) doubleValue();
    }

    @Override
    public long longValue() {
        return (long) doubleValue();
    }

    @Override
    public float floatValue() {
        return (float) doubleValue();
    }

    @Override
    public double doubleValue() {

        // Combine the 3 bytes into a 24-bit value
        int bits = ((bytes[0] & 0xFF) << 16) | ((bytes[1] & 0xFF) << 8) | (bytes[2] & 0xFF);

        // Extract components
        int sign = (bits >>> 23) & 0x01;
        int exponent = (bits >>> 16) & 0x7F;
        int mantissa = bits & 0xFFFF;

        // Handle special cases
        if (exponent == 0 && mantissa == 0) {
            // Zero
            return (sign == 0) ? 0.0 : -0.0;
        }

        if (exponent == 0x7F) {
            if (mantissa == 0) {
                // Infinity
                return (sign == 0) ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
            } else {
                // NaN
                return Double.NaN;
            }
        }

        // Convert to IEEE 754 double format
        // Adjust exponent to IEEE 754 bias (1023 instead of 63)
        int doubleExponent = exponent - FLOAT3_BIAS + DOUBLE_BIAS;

        // Extend mantissa from 16 bits to 52 bits
        long doubleMantissa = ((long) mantissa) << (52 - 16);

        // Combine components in IEEE 754 double format
        long doubleBits = ((long) sign << FLOAT3_BIAS) | ((long) doubleExponent << 52) | doubleMantissa;

        // Convert to double
        return Double.longBitsToDouble(doubleBits);
    }

}
