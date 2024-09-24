package org.huebert.iotfsdb.partition.adapter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class IntegerPartitionTest {

    private static final PartitionAdapter adapter = new IntegerPartition();

    @Test
    public void getTypeSize() {
        assertThat(adapter.getTypeSize()).isEqualTo(4);
    }

    @Test
    public void getAndSet() {

        int numBytes = 16;
        int numValues = 4;

        ByteBuffer buffer = ByteBuffer.allocate(numBytes);

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 4)).isEqualTo(0);
        }

        for (int i = 0; i < numValues; i++) {
            adapter.put(buffer, i * 4, i);
        }

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 4)).isEqualTo(i);
        }

        for (int i = 0; i < numValues; i++) {
            adapter.put(buffer, i * 4, null);
        }

        for (int i = 0; i < numValues; i++) {
            assertThat(adapter.get(buffer, i * 4)).isEqualTo(null);
        }
    }

}
