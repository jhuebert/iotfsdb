package org.huebert.iotfsdb.partition.adapter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class ShortPartitionTest {

    private static final PartitionAdapter adapter = new ShortPartition();

    @Test
    public void getTypeSize() {
        assertThat(adapter.getTypeSize()).isEqualTo(2);
    }

    @Test
    public void getAndSet() {

        int numBytes = 8;
        int numValues = 4;

        ByteBuffer buffer = ByteBuffer.allocate(numBytes);

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 2)).isEqualTo((short) 0);
        }

        for (int i = 0; i < numValues; i++) {
            adapter.put(buffer, i * 2, i);
        }

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 2)).isEqualTo((short) i);
        }

        for (int i = 0; i < numValues; i++) {
            adapter.put(buffer, i * 2, null);
        }

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 2)).isEqualTo(null);
        }
    }

}
