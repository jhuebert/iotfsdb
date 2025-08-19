package org.huebert.iotfsdb.partition;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class Float1Test {

    @Test
    public void testPositiveValue() {
        // Test a positive value: 0_1000_101 (sign=0, exp=8, mantissa=5)
        ByteBuffer buffer = getBuffer((byte) 0b0_1000_101);
        Float1 float1 = Float1.readFrom(buffer);
        testWriteTo(buffer, float1);
        assertThat(float1.isNaN()).isFalse();
        assertThat(float1.isInfinite()).isFalse();
        assertThat(float1.isPositiveInfinity()).isFalse();
        assertThat(float1.isNegativeInfinity()).isFalse();
        assertThat(float1.floatValue()).isGreaterThan(0.0f);
        assertThat(float1.doubleValue()).isGreaterThan(0.0);
    }

    @Test
    public void testNegativeValue() {
        // Test a negative value: 1_1000_101 (sign=1, exp=8, mantissa=5)
        ByteBuffer buffer = getBuffer((byte) 0b1_1000_101);
        Float1 float1 = Float1.readFrom(buffer);
        testWriteTo(buffer, float1);
        assertThat(float1.isNaN()).isFalse();
        assertThat(float1.isInfinite()).isFalse();
        assertThat(float1.isPositiveInfinity()).isFalse();
        assertThat(float1.isNegativeInfinity()).isFalse();
        assertThat(float1.floatValue()).isLessThan(0.0f);
        assertThat(float1.doubleValue()).isLessThan(0.0);
    }

    @Test
    public void testZero_Positive() {
        ByteBuffer buffer = getBuffer((byte) 0b0_0000_000);
        Float1 float1 = Float1.readFrom(buffer);
        testWriteTo(buffer, float1);
        assertThat(Float1.fromFloat(0.0f)).isEqualTo(float1);
        assertThat(Float1.fromDouble(0.0)).isEqualTo(float1);
        assertThat(Float1.fromDouble(0.0).hashCode()).isEqualTo(float1.hashCode());
        assertThat(float1.isNaN()).isFalse();
        assertThat(float1.isInfinite()).isFalse();
        assertThat(float1.isPositiveInfinity()).isFalse();
        assertThat(float1.isNegativeInfinity()).isFalse();
        assertThat(float1.intValue()).isEqualTo(0);
        assertThat(float1.longValue()).isEqualTo(0);
        assertThat(float1.floatValue()).isEqualTo(0.0f);
        assertThat(float1.doubleValue()).isEqualTo(0.0);
    }

    @Test
    public void testZero_Negative() {
        ByteBuffer buffer = getBuffer((byte) 0b1_0000_000);
        Float1 float1 = Float1.readFrom(buffer);
        testWriteTo(buffer, float1);
        assertThat(Float1.fromFloat(-0.0f)).isEqualTo(float1);
        assertThat(Float1.fromDouble(-0.0)).isEqualTo(float1);
        assertThat(Float1.fromDouble(-0.0).hashCode()).isEqualTo(float1.hashCode());
        assertThat(float1.isNaN()).isFalse();
        assertThat(float1.isInfinite()).isFalse();
        assertThat(float1.isPositiveInfinity()).isFalse();
        assertThat(float1.isNegativeInfinity()).isFalse();
        assertThat(float1.intValue()).isEqualTo(0);
        assertThat(float1.longValue()).isEqualTo(0);
        assertThat(float1.floatValue()).isEqualTo(-0.0f);
        assertThat(float1.doubleValue()).isEqualTo(-0.0);
    }

    @Test
    public void testPositiveInfinity() {
        ByteBuffer buffer = getBuffer((byte) 0b0_1111_000);
        Float1 float1 = Float1.readFrom(buffer);
        testWriteTo(buffer, float1);
        assertThat(Float1.fromFloat(Float.POSITIVE_INFINITY)).isEqualTo(float1);
        assertThat(Float1.fromDouble(Double.POSITIVE_INFINITY)).isEqualTo(float1);
        assertThat(float1.isNaN()).isFalse();
        assertThat(float1.isInfinite()).isTrue();
        assertThat(float1.isPositiveInfinity()).isTrue();
        assertThat(float1.isNegativeInfinity()).isFalse();
        assertThat(Float.isInfinite(float1.floatValue())).isTrue();
        assertThat(Double.isInfinite(float1.doubleValue())).isTrue();
        assertThat(float1.floatValue()).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(float1.doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testNegativeInfinity() {
        ByteBuffer buffer = getBuffer((byte) 0b1_1111_000);
        Float1 float1 = Float1.readFrom(buffer);
        testWriteTo(buffer, float1);
        assertThat(Float1.fromFloat(Float.NEGATIVE_INFINITY)).isEqualTo(float1);
        assertThat(Float1.fromDouble(Double.NEGATIVE_INFINITY)).isEqualTo(float1);
        assertThat(float1.isNaN()).isFalse();
        assertThat(float1.isInfinite()).isTrue();
        assertThat(float1.isPositiveInfinity()).isFalse();
        assertThat(float1.isNegativeInfinity()).isTrue();
        assertThat(Float.isInfinite(float1.floatValue())).isTrue();
        assertThat(Double.isInfinite(float1.doubleValue())).isTrue();
        assertThat(float1.floatValue()).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat(float1.doubleValue()).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testNaN() {
        ByteBuffer buffer = getBuffer((byte) 0b0_1111_111); // Use correct NaN pattern: 0x7F
        Float1 float1 = Float1.readFrom(buffer);
        testWriteTo(buffer, float1);
        assertThat(Float1.fromFloat(Float.NaN)).isEqualTo(float1);
        assertThat(Float1.fromDouble(Double.NaN)).isEqualTo(float1);
        assertThat(float1.isNaN()).isTrue();
        assertThat(float1.isInfinite()).isFalse();
        assertThat(float1.isPositiveInfinity()).isFalse();
        assertThat(float1.isNegativeInfinity()).isFalse();
        assertThat(Float.isNaN(float1.floatValue())).isTrue();
        assertThat(Double.isNaN(float1.doubleValue())).isTrue();
    }

    @Test
    public void testFromFloatConversion() {
        // Test conversion from various float values
        Float1 fromPositive = Float1.fromFloat(1.5f);
        assertThat(fromPositive.floatValue()).isGreaterThan(0.0f);

        Float1 fromNegative = Float1.fromFloat(-1.5f);
        assertThat(fromNegative.floatValue()).isLessThan(0.0f);

        Float1 fromZero = Float1.fromFloat(0.0f);
        assertThat(fromZero.floatValue()).isEqualTo(0.0f);

        Float1 fromNegativeZero = Float1.fromFloat(-0.0f);
        assertThat(fromNegativeZero.floatValue()).isEqualTo(-0.0f);
    }

    @Test
    public void testFromDoubleConversion() {
        // Test conversion from various double values
        Float1 fromPositive = Float1.fromDouble(1.5);
        assertThat(fromPositive.doubleValue()).isGreaterThan(0.0);

        Float1 fromNegative = Float1.fromDouble(-1.5);
        assertThat(fromNegative.doubleValue()).isLessThan(0.0);

        Float1 fromZero = Float1.fromDouble(0.0);
        assertThat(fromZero.doubleValue()).isEqualTo(0.0);

        Float1 fromNegativeZero = Float1.fromDouble(-0.0);
        assertThat(fromNegativeZero.doubleValue()).isEqualTo(-0.0);
    }

    @Test
    public void testEqualsAndHashCode() {
        Float1 float1a = Float1.fromFloat(1.5f);
        Float1 float1b = Float1.fromFloat(1.5f);
        Float1 float1c = Float1.fromFloat(2.0f);

        assertThat(float1a).isEqualTo(float1b);
        assertThat(float1a.hashCode()).isEqualTo(float1b.hashCode());
        assertThat(float1a).isNotEqualTo(float1c);
        assertThat(float1a).isNotEqualTo(null);
        assertThat(float1a).isNotEqualTo("not a Float1");
    }

    @Test
    public void testToString() {
        Float1 float1 = Float1.fromFloat(1.0f);
        String toString = float1.toString();
        assertThat(toString).contains("Float1");
        assertThat(toString).contains("value=");
    }

    @Test
    public void testConstants() {
        assertThat(Float1.BYTES).isEqualTo(1);
        assertThat(Float1.NaN.isNaN()).isTrue();
        assertThat(Float1.ZERO.floatValue()).isEqualTo(0.0f);
        assertThat(Float1.NEGATIVE_ZERO.floatValue()).isEqualTo(-0.0f);
        assertThat(Float1.POSITIVE_INFINITY.isPositiveInfinity()).isTrue();
        assertThat(Float1.NEGATIVE_INFINITY.isNegativeInfinity()).isTrue();
    }

    private ByteBuffer getBuffer(byte value) {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(value);
        buffer.flip();
        return buffer;
    }

    private void testWriteTo(ByteBuffer expectedBuffer, Float1 float1) {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        float1.writeTo(buffer);
        buffer.flip();
        expectedBuffer.rewind(); // Reset position to 0 for proper comparison
        assertThat(buffer).isEqualTo(expectedBuffer);
    }
}
