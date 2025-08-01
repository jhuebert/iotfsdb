package org.huebert.iotfsdb.partition;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class Float1PartitionTest {

    @Test
    public void testGetTypeSize() {
        Float1Partition partition = new Float1Partition();
        assertThat(partition.getTypeSize()).isEqualTo(1);
    }

    @Test
    public void testPutAndGetStream() {
        Float1Partition partition = new Float1Partition();
        ByteBuffer buffer = ByteBuffer.allocate(4); // 4 Float1 values

        // Put some values
        partition.put(buffer, 0, 1.5);
        partition.put(buffer, 1, -2.25);
        partition.put(buffer, 2, 0.0);
        partition.put(buffer, 3, null); // Should be NaN

        // Get the values back as a stream
        List<Number> values = partition.getStream(buffer, 0, 4)
            .collect(Collectors.toList());

        assertThat(values).hasSize(4);
        assertThat(values.get(0)).isInstanceOf(Float1.class);
        assertThat(values.get(0).floatValue()).isGreaterThan(0.0f);
        assertThat(values.get(1)).isInstanceOf(Float1.class);
        assertThat(values.get(1).floatValue()).isLessThan(0.0f);
        assertThat(values.get(2)).isInstanceOf(Float1.class);
        assertThat(values.get(2).floatValue()).isEqualTo(0.0f);
        assertThat(values.get(3)).isNull(); // NaN should become null
    }

    @Test
    public void testPutNull() {
        Float1Partition partition = new Float1Partition();
        ByteBuffer buffer = ByteBuffer.allocate(1);

        // Put null value
        partition.put(buffer, 0, null);

        // Get the value back
        List<Number> values = partition.getStream(buffer, 0, 1)
            .collect(Collectors.toList());

        assertThat(values).hasSize(1);
        assertThat(values.get(0)).isNull(); // Should be null because NaN is converted to null
    }

    @Test
    public void testSpecialValues() {
        Float1Partition partition = new Float1Partition();
        ByteBuffer buffer = ByteBuffer.allocate(3);

        // Test with infinity and NaN
        partition.put(buffer, 0, Float.POSITIVE_INFINITY);
        partition.put(buffer, 1, Float.NEGATIVE_INFINITY);
        partition.put(buffer, 2, Float.NaN);

        List<Number> values = partition.getStream(buffer, 0, 3)
            .collect(Collectors.toList());

        assertThat(values).hasSize(3);
        assertThat(values.get(0)).isInstanceOf(Float1.class);
        assertThat(Float.isInfinite(values.get(0).floatValue())).isTrue();
        assertThat(values.get(0).floatValue()).isPositive();

        assertThat(values.get(1)).isInstanceOf(Float1.class);
        assertThat(Float.isInfinite(values.get(1).floatValue())).isTrue();
        assertThat(values.get(1).floatValue()).isNegative();

        assertThat(values.get(2)).isNull(); // NaN becomes null
    }
}
