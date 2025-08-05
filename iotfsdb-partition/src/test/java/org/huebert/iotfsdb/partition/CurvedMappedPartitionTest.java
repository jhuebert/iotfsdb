package org.huebert.iotfsdb.partition;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class CurvedMappedPartitionTest {

    private static final int SIZE = 10;

    private static final int TYPE_SIZE = Short.BYTES;

    private static final int NUM_BYTES = SIZE * TYPE_SIZE;

    private final PartitionAdapter adapter = new CurvedMappedPartition(new ShortPartition(), -Short.MAX_VALUE, Short.MAX_VALUE);

    private ByteBuffer buffer;

    @BeforeEach
    public void beforeEach() {
        buffer = ByteBuffer.allocate(NUM_BYTES);
        for (int i = 0; i < SIZE; i++) {
            buffer.asShortBuffer().put(i, (short) i);
        }
    }

    @Test
    public void testGetTypeSize() {
        assertThat(adapter.getTypeSize()).isEqualTo(TYPE_SIZE);
    }

    @Test
    public void testPut() {
        adapter.put(buffer, 1, 1.234);
        assertThat(buffer.asShortBuffer().get(1)).isEqualTo((short) 1);
        adapter.put(buffer, 1, null);
        assertThat(buffer.asShortBuffer().get(1)).isEqualTo(Short.MIN_VALUE);
        adapter.put(buffer, 1, 50000);
        assertThat(buffer.asShortBuffer().get(1)).isEqualTo((short) 29809);
        adapter.put(buffer, 1, -50000);
        assertThat(buffer.asShortBuffer().get(1)).isEqualTo((short) -29809);
    }

    @Test
    public void testStream() {
        assertThat(adapter.getStream(buffer, 0, SIZE).mapToInt(Number::intValue).sum()).isEqualTo(45);
    }

}
