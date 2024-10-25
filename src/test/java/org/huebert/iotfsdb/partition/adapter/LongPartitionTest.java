package org.huebert.iotfsdb.partition.adapter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class LongPartitionTest {

    private static final PartitionAdapter adapter = new LongPartition();

    @Test
    public void getTypeSize() {
        assertThat(adapter.getTypeSize()).isEqualTo(8);
    }

    @Test
    public void getAndSet() {

        int numBytes = 32;
        int numValues = 4;

        ByteBuffer buffer = ByteBuffer.allocate(numBytes);

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 8)).isEqualTo(0L);
        }

        for (int i = 0; i < numValues; i++) {
            adapter.put(buffer, i * 8, i);
        }

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 8)).isEqualTo((long) i);
        }

        for (int i = 0; i < numValues; i++) {
            adapter.put(buffer, i * 8, null);
        }

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 8)).isEqualTo(null);
        }
    }

}
