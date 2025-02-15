package org.huebert.iotfsdb.partition;

import org.huebert.iotfsdb.collectors.SummingCollector;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RangeMapperTest {

    @Test
    void testZeroDecodedRange() {
        assertThrows(IllegalArgumentException.class, () -> new RangeMapper(0.0, 0.0, 0.0, 1.0, false));
    }

    @Test
    void testZeroEncodedRange() {
        assertThrows(IllegalArgumentException.class, () -> new RangeMapper(0.0, 1.0, 0.0, 0.0, false));
    }

    @Test
    void testInvalidDecodedRange() {
        assertThrows(IllegalArgumentException.class, () -> new RangeMapper(1.0, 0.0, 0.0, 1.0, false));
    }

    @Test
    void testInvalidEncodedRange() {
        assertThrows(IllegalArgumentException.class, () -> new RangeMapper(0.0, 1.0, 1.0, 0.0, false));
    }

    @Test
    void testEncodeNoConstrain() {
        RangeMapper mapper = new RangeMapper(-1.0, 1.0, 4.0, 8.0, false);
        assertThat(mapper.encode(-2.0)).isEqualTo(2.0);
        assertThat(mapper.encode(-1.0)).isEqualTo(4.0);
        assertThat(mapper.encode(0.0)).isEqualTo(6.0);
        assertThat(mapper.encode(1.0)).isEqualTo(8.0);
        assertThat(mapper.encode(2.0)).isEqualTo(10.0);
    }

    @Test
    void testEncodeWithConstrain() {
        RangeMapper mapper = new RangeMapper(-1.0, 1.0, 4.0, 8.0, true);
        assertThat(mapper.encode(-2.0)).isEqualTo(4.0);
        assertThat(mapper.encode(-1.0)).isEqualTo(4.0);
        assertThat(mapper.encode(0.0)).isEqualTo(6.0);
        assertThat(mapper.encode(1.0)).isEqualTo(8.0);
        assertThat(mapper.encode(2.0)).isEqualTo(8.0);
    }

    @Test
    void testDecodeNoConstrain() {
        RangeMapper mapper = new RangeMapper(-1.0, 1.0, 4.0, 8.0, false);
        assertThat(mapper.decode(2.0)).isEqualTo(-2.0);
        assertThat(mapper.decode(4.0)).isEqualTo(-1.0);
        assertThat(mapper.decode(6.0)).isEqualTo(0.0);
        assertThat(mapper.decode(8.0)).isEqualTo(1.0);
        assertThat(mapper.decode(10.0)).isEqualTo(2.0);
    }

    @Test
    void testDecodeWithConstrain() {
        RangeMapper mapper = new RangeMapper(-1.0, 1.0, 4.0, 8.0, true);
        assertThat(mapper.decode(2.0)).isEqualTo(-1.0);
        assertThat(mapper.decode(4.0)).isEqualTo(-1.0);
        assertThat(mapper.decode(6.0)).isEqualTo(0.0);
        assertThat(mapper.decode(8.0)).isEqualTo(1.0);
        assertThat(mapper.decode(10.0)).isEqualTo(1.0);
    }

}
