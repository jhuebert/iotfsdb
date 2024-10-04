package org.huebert.iotfsdb.series.adapter;

import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class FloatTypeAdapterTest {

    private final FloatTypeAdapter adapter = new FloatTypeAdapter();

    @Test
    public void testConvert() {
        assertThat(adapter.convert(null)).isEqualTo(null);
        assertThat(adapter.convert("0")).isEqualTo(0);
        assertThat(adapter.convert("-1")).isEqualTo(-1);
        assertThat(adapter.convert("1")).isEqualTo(1);
    }

    @Test
    public void testAggregateEmpty() {
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.AVERAGE)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.COUNT)).isEqualTo(Optional.of(0.0f));
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.FIRST)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.LAST)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.MAXIMUM)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.MEDIAN)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.MINIMUM)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.SUM)).isEqualTo(Optional.empty());
    }

    @Test
    public void testAggregate() {

        List<Float> input = new ArrayList<>();
        input.add(3.0f);
        input.add(1.0f);
        input.add(1.0f);
        input.add(4.0f);
        input.add(0.0f);

        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.AVERAGE)).isEqualTo(Optional.of(1.8f));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.COUNT)).isEqualTo(Optional.of(5.0f));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.FIRST)).isEqualTo(Optional.of(3.0f));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.LAST)).isEqualTo(Optional.of(0.0f));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.MAXIMUM)).isEqualTo(Optional.of(4.0f));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.MEDIAN)).isEqualTo(Optional.of(1.0f));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.MINIMUM)).isEqualTo(Optional.of(0.0f));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.SUM)).isEqualTo(Optional.of(9.0f));
    }

    @Test
    public void testCreate() throws Exception {
        File temp = File.createTempFile(FloatTypeAdapterTest.class.getSimpleName(), "");
        temp.deleteOnExit();
        temp.delete();
        try (FileBasedArray<Float> array = adapter.create(temp, 10)) {
            assertThat(array.size()).isEqualTo(10);
        }
        temp.delete();
    }

    @Test
    public void testRead() throws Exception {
        File temp = File.createTempFile(FloatTypeAdapterTest.class.getSimpleName(), "");
        temp.deleteOnExit();
        RandomAccessFile raf = new RandomAccessFile(temp, "rw");
        raf.setLength(40);
        raf.close();
        try (FileBasedArray<Float> array = adapter.read(temp, false)) {
            assertThat(array.size()).isEqualTo(10);
        }
        temp.delete();
    }
}
