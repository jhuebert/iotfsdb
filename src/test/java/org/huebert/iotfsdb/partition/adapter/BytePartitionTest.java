package org.huebert.iotfsdb.partition.adapter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class BytePartitionTest {

    private static final PartitionAdapter adapter = new BytePartition();

    @Test
    public void getTypeSize() {
        assertThat(adapter.getTypeSize()).isEqualTo(1);
    }

    @Test
    public void getAndSet() {

        int numBytes = 4;
        int numValues = numBytes;

        ByteBuffer buffer = ByteBuffer.allocate(numBytes);

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i)).isEqualTo((byte) 0);
        }

        for (int i = 0; i < numValues; i++) {
            adapter.put(buffer, i, i);
        }

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i)).isEqualTo((byte) i);
        }

        for (int i = 0; i < numValues; i++) {
            adapter.put(buffer, i, null);
        }

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i)).isEqualTo(null);
        }
    }

}
