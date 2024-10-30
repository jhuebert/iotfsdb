package org.huebert.iotfsdb.partition.adapter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class FixedPartitionTest {

    @Test
    public void getTypeSize() {
        FixedPartition adapter1 = new FixedPartition(new BytePartition(), 0.0, 1.0);
        assertThat(adapter1.getTypeSize()).isEqualTo(1);
        FixedPartition adapter2 = new FixedPartition(new ShortPartition(), 0.0, 1.0);
        assertThat(adapter2.getTypeSize()).isEqualTo(2);
        FixedPartition adapter4 = new FixedPartition(new IntegerPartition(), 0.0, 1.0);
        assertThat(adapter4.getTypeSize()).isEqualTo(4);
    }

    @Test
    public void getAndSetByte() {

        FixedPartition adapter = new FixedPartition(new BytePartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(1);

        adapter.put(buffer, 0, -26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-24.015748031496063);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-0.984251968503937);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.9842519685039335);

        adapter.put(buffer, 0, 24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(24.015748031496067);

        adapter.put(buffer, 0, 25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.0);

        adapter.put(buffer, 0, 26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(25.0);

        adapter.put(buffer, 0, null);
        assertThat(adapter.get(buffer, 0)).isEqualTo(null);
    }

    @Test
    public void getAndSetShort() {

        FixedPartition adapter = new FixedPartition(new ShortPartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(2);

        adapter.put(buffer, 0, -26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-23.999755851924192);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-1.000244148075808);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(1.000244148075808);

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

        FixedPartition adapter = new FixedPartition(new IntegerPartition(), -25, 25);

        ByteBuffer buffer = ByteBuffer.allocate(4);

        adapter.put(buffer, 0, -26.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -25.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-25.0);

        adapter.put(buffer, 0, -24.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-23.999999998603016);

        adapter.put(buffer, 0, -1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(-1.0000000013969839);

        adapter.put(buffer, 0, 0.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(0.0);

        adapter.put(buffer, 0, 1.0);
        assertThat(adapter.get(buffer, 0)).isEqualTo(1.0000000013969839);

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
