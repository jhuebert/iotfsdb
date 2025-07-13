package org.huebert.iotfsdb.partition;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class Float3PartitionTest {

    private static final int SIZE = 10;

    private static final int TYPE_SIZE = Float3.BYTES;

    private static final int NUM_BYTES = SIZE * TYPE_SIZE;

    private final PartitionAdapter adapter = new Float3Partition();

    private ByteBuffer buffer;

    @BeforeEach
    public void beforeEach() {
        buffer = ByteBuffer.allocate(NUM_BYTES);
        for (int i = 0; i < SIZE; i++) {
            Float3.fromFloat(i).writeTo(buffer);
        }
    }

    @Test
    public void testGetTypeSize() {
        assertThat(adapter.getTypeSize()).isEqualTo(TYPE_SIZE);
    }

    @Test
    public void testPut() {
        adapter.put(buffer, 1, 1.234);

        assertThat(Float3.readFrom(buffer.slice(TYPE_SIZE, TYPE_SIZE)).floatValue()).isEqualTo(1.2339935f);
        adapter.put(buffer, 1, null);
        assertThat(Float3.readFrom(buffer.slice(TYPE_SIZE, TYPE_SIZE)).floatValue()).isNaN();
    }

    @Test
    public void testStream() {
        assertThat(adapter.getStream(buffer, 0, SIZE).mapToInt(Number::intValue).sum()).isEqualTo(45);
    }

}
