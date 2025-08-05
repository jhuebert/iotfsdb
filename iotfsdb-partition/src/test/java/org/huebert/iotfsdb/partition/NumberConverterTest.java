package org.huebert.iotfsdb.partition;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class NumberConverterTest {

    @Test
    public void testFromByte() {
        assertThat(NumberConverter.fromByte(Byte.MIN_VALUE)).isEqualTo(null);
        assertThat(NumberConverter.fromByte((byte) 1)).isEqualTo((byte) 1);
    }

    @Test
    public void testToByte() {
        assertThat(NumberConverter.toByte(null)).isEqualTo(Byte.MIN_VALUE);
        assertThat(NumberConverter.toByte(1.234)).isEqualTo((byte) 1);
    }

    @Test
    public void testFromDouble() {
        assertThat(NumberConverter.fromDouble(Double.NaN)).isEqualTo(null);
        assertThat(NumberConverter.fromDouble(1.234)).isEqualTo(1.234);
    }

    @Test
    public void testToDouble() {
        assertThat(NumberConverter.toDouble(null)).isNaN();
        assertThat(NumberConverter.toDouble(1.234)).isEqualTo(1.234);
    }

    @Test
    public void testFromFloat() {
        assertThat(NumberConverter.fromFloat(Float.NaN)).isEqualTo(null);
        assertThat(NumberConverter.fromFloat(1.234f)).isEqualTo(1.234f);
    }

    @Test
    public void testToFloat() {
        assertThat(NumberConverter.toFloat(null)).isNaN();
        assertThat(NumberConverter.toFloat(1.234)).isEqualTo(1.234f);
    }

    @Test
    public void testFromHalfFloat() {
        assertThat(NumberConverter.fromHalfFloat(Float.floatToFloat16(Float.NaN))).isEqualTo(null);
        assertThat(NumberConverter.fromHalfFloat(Float.floatToFloat16(1.234f))).isEqualTo(1.234375f);
    }

    @Test
    public void testToHalfFloat() {
        assertThat(NumberConverter.toHalfFloat(null)).isEqualTo((short) 32256);
        assertThat(NumberConverter.toHalfFloat(1.234)).isEqualTo(Float.floatToFloat16(1.234f));
    }

    @Test
    public void testFromInt() {
        assertThat(NumberConverter.fromInt(Integer.MIN_VALUE)).isEqualTo(null);
        assertThat(NumberConverter.fromInt(1)).isEqualTo(1);
    }

    @Test
    public void testToInt() {
        assertThat(NumberConverter.toInt(null)).isEqualTo(Integer.MIN_VALUE);
        assertThat(NumberConverter.toInt(1.234)).isEqualTo(1);
    }

    @Test
    public void testFromLong() {
        assertThat(NumberConverter.fromLong(Long.MIN_VALUE)).isEqualTo(null);
        assertThat(NumberConverter.fromLong(1)).isEqualTo((long) 1);
    }

    @Test
    public void testToLong() {
        assertThat(NumberConverter.toLong(null)).isEqualTo(Long.MIN_VALUE);
        assertThat(NumberConverter.toLong(1.234)).isEqualTo(1);
    }

    @Test
    public void testFromShort() {
        assertThat(NumberConverter.fromShort(Short.MIN_VALUE)).isEqualTo(null);
        assertThat(NumberConverter.fromShort((short) 1)).isEqualTo((short) 1);
    }

    @Test
    public void testToShort() {
        assertThat(NumberConverter.toShort(null)).isEqualTo(Short.MIN_VALUE);
        assertThat(NumberConverter.toShort(1.234)).isEqualTo((short) 1);
    }

}
