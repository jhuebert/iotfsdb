package org.huebert.iotfsdb.partition.adapter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class CurvedMappedPartitionTest {

    @Test
    public void getTypeSize() {
        CurvedMappedPartition adapter1 = new CurvedMappedPartition(new BytePartition(), 0.0, 1.0);
        assertThat(adapter1.getTypeSize()).isEqualTo(1);
        CurvedMappedPartition adapter2 = new CurvedMappedPartition(new ShortPartition(), 0.0, 1.0);
        assertThat(adapter2.getTypeSize()).isEqualTo(2);
        CurvedMappedPartition adapter4 = new CurvedMappedPartition(new IntegerPartition(), 0.0, 1.0);
        assertThat(adapter4.getTypeSize()).isEqualTo(4);
    }

    @Test
    public void getAndSetByte() {

        CurvedMappedPartition adapter = new CurvedMappedPartition(new BytePartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(1);

        adapter.put(buffer, 0, -100.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-69.16736860909403);

        adapter.put(buffer, 0, -75.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-69.16736860909403);

        adapter.put(buffer, 0, -50.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-48.85018730038257);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.13060837741105);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-0.9847609731639295);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.9847609731639295);

        adapter.put(buffer, 0, 25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.130608377411058);

        adapter.put(buffer, 0, 50.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(48.85018730038257);

        adapter.put(buffer, 0, 75.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(69.16736860909403);

        adapter.put(buffer, 0, 100.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(69.16736860909403);

        adapter.put(buffer, 0, null);
        assertThat(adapter.get(buffer, 0)).isEqualTo(null);
    }

    @Test
    public void getAndSetShort() {

        CurvedMappedPartition adapter = new CurvedMappedPartition(new ShortPartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(2);

        adapter.put(buffer, 0, -100.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-75.00316219975713);

        adapter.put(buffer, 0, -75.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-75.00316219975713);

        adapter.put(buffer, 0, -50.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-49.996850119326346);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-24.999717129069865);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-1.000014200256338);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(1.000014200256338);

        adapter.put(buffer, 0, 25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(24.999717129069865);

        adapter.put(buffer, 0, 50.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(49.99685011932635);

        adapter.put(buffer, 0, 75.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(75.00316219975713);

        adapter.put(buffer, 0, 100.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(75.00316219975713);

        adapter.put(buffer, 0, null);
        assertThat(adapter.get(buffer, 0)).isEqualTo(null);
    }

    @Test
    public void getAndSetInt() {

        CurvedMappedPartition adapter = new CurvedMappedPartition(new IntegerPartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(4);

        adapter.put(buffer, 0, -100.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-74.99999951401372);

        adapter.put(buffer, 0, -75.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-74.99999951401372);

        adapter.put(buffer, 0, -50.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-49.99999992258847);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.00000001203661);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-0.9999999977045277);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.9999999977045277);

        adapter.put(buffer, 0, 25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.000000012036615);

        adapter.put(buffer, 0, 50.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(49.99999992258847);

        adapter.put(buffer, 0, 75.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(74.99999951401372);

        adapter.put(buffer, 0, 100.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(74.99999951401372);

        adapter.put(buffer, 0, null);
        assertThat(adapter.get(buffer, 0)).isEqualTo(null);
    }

}
