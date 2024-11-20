package org.huebert.iotfsdb.partition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class HalfFloatPartitionTest {

    private static final int SIZE = 10;

    private static final int TYPE_SIZE = Short.BYTES;

    private static final int NUM_BYTES = SIZE * TYPE_SIZE;

    private final PartitionAdapter adapter = new HalfFloatPartition();

    private ByteBuffer buffer;

    @BeforeEach
    public void beforeEach() {
        buffer = ByteBuffer.allocate(NUM_BYTES);
        for (int i = 0; i < SIZE; i++) {
            buffer.asShortBuffer().put(i, Float.floatToFloat16(i));
        }
    }

    @Test
    public void testGetTypeSize() {
        assertThat(adapter.getTypeSize()).isEqualTo(TYPE_SIZE);
    }

    @Test
    public void testPut() {
        adapter.put(buffer, 1, 1.234);
        assertThat(Float.float16ToFloat(buffer.asShortBuffer().get(1))).isEqualTo(1.234375f);
        adapter.put(buffer, 1, null);
        assertThat(Float.float16ToFloat(buffer.asShortBuffer().get(1))).isNaN();
    }

    @Test
    public void testStream() {
        assertThat(adapter.getStream(buffer, 0, SIZE).mapToInt(Number::intValue).sum()).isEqualTo(45);
    }

}