package org.huebert.iotfsdb.partition;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class Float3Test {

    @Test
    public void testPi_Positive() {
        ByteBuffer buffer = getBuffer(0b0_1000000_10010010_00011111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat((float) Math.PI)).isEqualTo(float3);
        assertThat(Float3.fromDouble(Math.PI)).isEqualTo(float3);
        assertThat(Float3.fromDouble(Math.PI).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(3);
        assertThat(float3.longValue()).isEqualTo(3);
        assertThat(float3.floatValue()).isEqualTo(3.141571f);
        assertThat(float3.doubleValue()).isEqualTo(3.141571f);
    }

    @Test
    public void testPi_Negative() {
        ByteBuffer buffer = getBuffer(0b1_1000000_10010010_00011111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat((float) -Math.PI)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-Math.PI)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-Math.PI).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(-3);
        assertThat(float3.longValue()).isEqualTo(-3);
        assertThat(float3.floatValue()).isEqualTo(-3.141571f);
        assertThat(float3.doubleValue()).isEqualTo(-3.141571f);
    }

    @Test
    public void testZero_Positive() {
        ByteBuffer buffer = getBuffer(0b0_0000000_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(0.0f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(0.0)).isEqualTo(float3);
        assertThat(Float3.fromDouble(0.0).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(0.0f);
        assertThat(float3.doubleValue()).isEqualTo(0.0f);
    }

    @Test
    public void testZero_Negative() {
        ByteBuffer buffer = getBuffer(0b1_0000000_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-0.0f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-0.0)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-0.0).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(-0.0f);
        assertThat(float3.doubleValue()).isEqualTo(-0.0f);
    }

    @Test
    public void testSmallestSubnormal_Positive() {
        ByteBuffer buffer = getBuffer(0b0_0000000_00000000_00000001);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(1.0842187E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.0842187E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.0842187E-19f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(1.0842187E-19f);
        assertThat(float3.doubleValue()).isEqualTo(1.0842187E-19f);
    }

    @Test
    public void testSmallestSubnormal_Negative() {
        ByteBuffer buffer = getBuffer(0b1_0000000_00000000_00000001);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-1.0842187E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.0842187E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.0842187E-19f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(-1.0842187E-19f);
        assertThat(float3.doubleValue()).isEqualTo(-1.0842187E-19f);
    }

    @Test
    public void testLargestSubnormal_Positive() {
        ByteBuffer buffer = getBuffer(0b0_0000000_11111111_11111111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(2.1683878E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(2.1683878E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(2.1683878E-19f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(2.1683878E-19f);
        assertThat(float3.doubleValue()).isEqualTo(2.1683878E-19f);
    }

    @Test
    public void testLargestSubnormal_Negative() {
        ByteBuffer buffer = getBuffer(0b1_0000000_11111111_11111111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-2.1683878E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-2.1683878E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-2.1683878E-19f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(-2.1683878E-19f);
        assertThat(float3.doubleValue()).isEqualTo(-2.1683878E-19f);
    }

    @Test
    public void testSmallestPositiveNormal_Positive() {
        ByteBuffer buffer = getBuffer(0b0_0000001_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(2.1684043E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(2.1684043E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(2.1684043E-19f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(2.1684043E-19f);
        assertThat(float3.doubleValue()).isEqualTo(2.1684043E-19f);
    }

    @Test
    public void testSmallestPositiveNormal_Negative() {
        ByteBuffer buffer = getBuffer(0b1_0000001_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-2.1684043E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-2.1684043E-19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-2.1684043E-19f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(-2.1684043E-19f);
        assertThat(float3.doubleValue()).isEqualTo(-2.1684043E-19f);
    }

    @Test
    public void testOneThird_Positive() {
        ByteBuffer buffer = getBuffer(0b0_0111101_01010101_01010101);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(1.0f / 3.0f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.0 / 3.0)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.0 / 3.0).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(0.33333206f);
        assertThat(float3.doubleValue()).isEqualTo(0.33333206f);
    }

    @Test
    public void testOneThird_Negative() {
        ByteBuffer buffer = getBuffer(0b1_0111101_01010101_01010101);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-1.0f / 3.0f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.0 / 3.0)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.0 / 3.0).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(-0.33333206f);
        assertThat(float3.doubleValue()).isEqualTo(-0.33333206f);
    }

    @Test
    public void testLargestNumberLessThanOne_Positive() {
        ByteBuffer buffer = getBuffer(0b0_0111110_11111111_11111111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(0.9999924f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(0.9999924f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(0.9999924f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(0.9999924f);
        assertThat(float3.doubleValue()).isEqualTo(0.9999924f);
    }

    @Test
    public void testLargestNumberLessThanOne_Negative() {
        ByteBuffer buffer = getBuffer(0b1_0111110_11111111_11111111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-0.9999924f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-0.9999924f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-0.9999924f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isEqualTo(-0.9999924f);
        assertThat(float3.doubleValue()).isEqualTo(-0.9999924f);
    }

    @Test
    public void testOne_Positive() {
        ByteBuffer buffer = getBuffer(0b0_0111111_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(1.0f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.0)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.0).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(1);
        assertThat(float3.longValue()).isEqualTo(1);
        assertThat(float3.floatValue()).isEqualTo(1.0f);
        assertThat(float3.doubleValue()).isEqualTo(1.0f);
    }

    @Test
    public void testOne_Negative() {
        ByteBuffer buffer = getBuffer(0b1_0111111_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-1.0f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.0)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.0).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(-1);
        assertThat(float3.longValue()).isEqualTo(-1);
        assertThat(float3.floatValue()).isEqualTo(-1.0f);
        assertThat(float3.doubleValue()).isEqualTo(-1.0f);
    }

    @Test
    public void testSmallestNumberLargerThanOne_Positive() {
        ByteBuffer buffer = getBuffer(0b0_0111111_00000000_00000001);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(1.0000153f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.0000153f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.0000153f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(1);
        assertThat(float3.longValue()).isEqualTo(1);
        assertThat(float3.floatValue()).isEqualTo(1.0000153f);
        assertThat(float3.doubleValue()).isEqualTo(1.0000153f);
    }

    @Test
    public void testSmallestNumberLargerThanOne_Negative() {
        ByteBuffer buffer = getBuffer(0b1_0111111_00000000_00000001);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-1.0000153f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.0000153f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.0000153f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(-1);
        assertThat(float3.longValue()).isEqualTo(-1);
        assertThat(float3.floatValue()).isEqualTo(-1.0000153f);
        assertThat(float3.doubleValue()).isEqualTo(-1.0000153f);
    }

    @Test
    public void testLargestNormalNumber_Positive() {
        ByteBuffer buffer = getBuffer(0b0_1111110_11111111_11111111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(1.8446603E19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.8446603E19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(1.8446603E19f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(2147483647);
        assertThat(float3.longValue()).isEqualTo(9223372036854775807L);
        assertThat(float3.floatValue()).isEqualTo(1.8446603E19f);
        assertThat(float3.doubleValue()).isEqualTo(1.8446603E19f);
    }

    @Test
    public void testLargestNormalNumber_Negative() {
        ByteBuffer buffer = getBuffer(0b1_1111110_11111111_11111111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(-1.8446603E19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.8446603E19f)).isEqualTo(float3);
        assertThat(Float3.fromDouble(-1.8446603E19f).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(-2147483648);
        assertThat(float3.longValue()).isEqualTo(-9223372036854775808L);
        assertThat(float3.floatValue()).isEqualTo(-1.8446603E19f);
        assertThat(float3.doubleValue()).isEqualTo(-1.8446603E19f);
    }

    @Test
    public void testInfinity_Positive() {
        ByteBuffer buffer = getBuffer(0b0_1111111_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(Float.POSITIVE_INFINITY)).isEqualTo(float3);
        assertThat(Float3.fromDouble(Float.POSITIVE_INFINITY)).isEqualTo(float3);
        assertThat(Float3.fromDouble(Float.POSITIVE_INFINITY).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isTrue();
        assertThat(float3.isPositiveInfinity()).isTrue();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(2147483647);
        assertThat(float3.longValue()).isEqualTo(9223372036854775807L);
        assertThat(float3.floatValue()).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(float3.doubleValue()).isEqualTo(Float.POSITIVE_INFINITY);
    }

    @Test
    public void testInfinity_FromFloat() {
        float value = 2.5e30f;
        assertThat(Float3.fromFloat(value)).isEqualTo(Float3.POSITIVE_INFINITY);
        assertThat(Float3.fromFloat(value).isPositiveInfinity()).isTrue();
        assertThat(Float3.fromFloat(value).isNegativeInfinity()).isFalse();
        assertThat(Float3.fromFloat(-value)).isEqualTo(Float3.NEGATIVE_INFINITY);
        assertThat(Float3.fromFloat(-value).isPositiveInfinity()).isFalse();
        assertThat(Float3.fromFloat(-value).isNegativeInfinity()).isTrue();
    }

    @Test
    public void testZero_FromFloat() {
        float value = 2.5e-30f;
        assertThat(Float3.fromFloat(value)).isEqualTo(Float3.ZERO);
        assertThat(Float3.fromFloat(value).floatValue()).isEqualTo(0.0f);
        assertThat(Float3.fromFloat(-value)).isEqualTo(Float3.NEGATIVE_ZERO);
        assertThat(Float3.fromFloat(-value).floatValue()).isEqualTo(-0.0f);
    }

    @Test
    public void testInfinity_FromDouble() {
        double value = 2.5e30;
        assertThat(Float3.fromDouble(value)).isEqualTo(Float3.POSITIVE_INFINITY);
        assertThat(Float3.fromDouble(value).isPositiveInfinity()).isTrue();
        assertThat(Float3.fromDouble(value).isNegativeInfinity()).isFalse();
        assertThat(Float3.fromDouble(-value)).isEqualTo(Float3.NEGATIVE_INFINITY);
        assertThat(Float3.fromDouble(-value).isPositiveInfinity()).isFalse();
        assertThat(Float3.fromDouble(-value).isNegativeInfinity()).isTrue();
    }

    @Test
    public void testZero_FromDouble() {
        double value = 2.5e-30;
        assertThat(Float3.fromDouble(value)).isEqualTo(Float3.ZERO);
        assertThat(Float3.fromDouble(value).doubleValue()).isEqualTo(0.0);
        assertThat(Float3.fromDouble(-value)).isEqualTo(Float3.NEGATIVE_ZERO);
        assertThat(Float3.fromDouble(-value).doubleValue()).isEqualTo(-0.0);
    }

    @Test
    public void testInfinity_Negative() {
        ByteBuffer buffer = getBuffer(0b1_1111111_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(Float.NEGATIVE_INFINITY)).isEqualTo(float3);
        assertThat(Float3.fromDouble(Float.NEGATIVE_INFINITY)).isEqualTo(float3);
        assertThat(Float3.fromDouble(Float.NEGATIVE_INFINITY).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isFalse();
        assertThat(float3.isInfinite()).isTrue();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isTrue();
        assertThat(float3.intValue()).isEqualTo(-2147483648);
        assertThat(float3.longValue()).isEqualTo(-9223372036854775808L);
        assertThat(float3.floatValue()).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat(float3.doubleValue()).isEqualTo(Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testNaN() {
        ByteBuffer buffer = getBuffer(0b0_1111111_11111111_11111111);
        Float3 float3 = Float3.readFrom(buffer);
        testWriteTo(buffer, float3);
        assertThat(Float3.fromFloat(Float.NaN)).isEqualTo(float3);
        assertThat(Float3.fromDouble(Double.NaN)).isEqualTo(float3);
        assertThat(Float3.fromDouble(Double.NaN).hashCode()).isEqualTo(float3.hashCode());
        assertThat(float3.isNaN()).isTrue();
        assertThat(float3.isInfinite()).isFalse();
        assertThat(float3.isPositiveInfinity()).isFalse();
        assertThat(float3.isNegativeInfinity()).isFalse();
        assertThat(float3.intValue()).isEqualTo(0);
        assertThat(float3.longValue()).isEqualTo(0);
        assertThat(float3.floatValue()).isNaN();
        assertThat(float3.doubleValue()).isNaN();
    }

    @Test
    public void testToString() {
        ByteBuffer buffer = getBuffer(0b0_1000000_10010010_00011111);
        Float3 float3 = Float3.readFrom(buffer);
        assertThat(float3.toString()).isEqualTo("Float3{bytes=[64, -110, 31]}");
    }

    @Test
    public void testEquals() {
        ByteBuffer buffer = getBuffer(0b0_0000000_00000000_00000000);
        Float3 float3 = Float3.readFrom(buffer);

        assertThat(Float3.readFrom(getBuffer(0b0_0000000_00000000_00000000))).isEqualTo(float3);
        assertThat(Float3.readFrom(getBuffer(0b1_0000000_00000000_00000000))).isNotEqualTo(float3);
        assertThat(Float3.readFrom(getBuffer(0b0_0000001_00000000_00000000))).isNotEqualTo(float3);
        assertThat(Float3.readFrom(getBuffer(0b0_0000000_00000001_00000000))).isNotEqualTo(float3);
        assertThat(Float3.readFrom(getBuffer(0b0_0000000_00000000_00000001))).isNotEqualTo(float3);

        assertThat("").isNotEqualTo(float3);
    }

    private static void testWriteTo(ByteBuffer original, Float3 float3) {
        ByteBuffer buffer = ByteBuffer.allocate(3);
        float3.writeTo(buffer);
        original.position(0);
        buffer.position(0);
        assertThat(buffer.get()).isEqualTo(original.get());
        assertThat(buffer.get()).isEqualTo(original.get());
        assertThat(buffer.get()).isEqualTo(original.get());
    }

    private static ByteBuffer getBuffer(int value) {
        return getBuffer(
            (byte) ((value >>> 16) & 0xFF),
            (byte) ((value >>> 8) & 0xFF),
            (byte) (value & 0xFF)
        );
    }

    private static ByteBuffer getBuffer(byte a, byte b, byte c) {
        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(a);
        buffer.put(b);
        buffer.put(c);
        buffer.position(0);
        return buffer;
    }

}
