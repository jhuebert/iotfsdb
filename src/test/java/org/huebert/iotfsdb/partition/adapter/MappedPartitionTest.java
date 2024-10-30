package org.huebert.iotfsdb.partition.adapter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class MappedPartitionTest {

    @Test
    public void getTypeSize() {
        MappedPartition adapter1 = new MappedPartition(new BytePartition(), 0.0, 1.0);
        assertThat(adapter1.getTypeSize()).isEqualTo(1);
        MappedPartition adapter2 = new MappedPartition(new ShortPartition(), 0.0, 1.0);
        assertThat(adapter2.getTypeSize()).isEqualTo(2);
        MappedPartition adapter4 = new MappedPartition(new IntegerPartition(), 0.0, 1.0);
        assertThat(adapter4.getTypeSize()).isEqualTo(4);
    }

    @Test
    public void getAndSetByte() {

        MappedPartition adapter = new MappedPartition(new BytePartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(1);

        adapter.put(buffer, 0, -26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-23.818897637795274);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-0.984251968503937);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.984251968503937);

        adapter.put(buffer, 0, 24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(23.818897637795274);

        adapter.put(buffer, 0, 25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.0);

        adapter.put(buffer, 0, 26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.0);

        adapter.put(buffer, 0, null);
        assertThat(adapter.get(buffer, 0)).isEqualTo(null);
    }

    @Test
    public void getAndSetShort() {

        MappedPartition adapter = new MappedPartition(new ShortPartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(2);

        adapter.put(buffer, 0, -26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-23.999755851924192);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-0.9994811853389081);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.9994811853389081);

        adapter.put(buffer, 0, 24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(23.999755851924192);

        adapter.put(buffer, 0, 25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.0);

        adapter.put(buffer, 0, 26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.0);

        adapter.put(buffer, 0, null);
        assertThat(adapter.get(buffer, 0)).isEqualTo(null);
    }

    @Test
    public void getAndSetInt() {

        MappedPartition adapter = new MappedPartition(new IntegerPartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(4);

        adapter.put(buffer, 0, -26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-23.999999998603016);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-0.9999999897554517);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.9999999897554517);

        adapter.put(buffer, 0, 24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(23.999999998603016);

        adapter.put(buffer, 0, 25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.0);

        adapter.put(buffer, 0, 26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.0);

        adapter.put(buffer, 0, null);
        assertThat(adapter.get(buffer, 0)).isEqualTo(null);
    }

}
