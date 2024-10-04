package org.huebert.iotfsdb.series.adapter;

import org.huebert.iotfsdb.file.BooleanFileBasedArrayTest;
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

public class BooleanTypeAdapterTest {

    private final BooleanTypeAdapter adapter = new BooleanTypeAdapter();

    @Test
    public void testConvert() {
        assertThat(adapter.convert(null)).isEqualTo(null);
        assertThat(adapter.convert("")).isEqualTo(false);
        assertThat(adapter.convert(" ")).isEqualTo(false);
        assertThat(adapter.convert("no")).isEqualTo(false);
        assertThat(adapter.convert("yes")).isEqualTo(false);
        assertThat(adapter.convert("false")).isEqualTo(false);
        assertThat(adapter.convert("FALSE")).isEqualTo(false);
        assertThat(adapter.convert("true")).isEqualTo(true);
        assertThat(adapter.convert("TRUE")).isEqualTo(true);
    }

    @Test
    public void testAggregateEmpty() {
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.AVERAGE)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.COUNT)).isEqualTo(Optional.of(false));
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.FIRST)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.LAST)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.MAXIMUM)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.MEDIAN)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.MINIMUM)).isEqualTo(Optional.empty());
        assertThat(adapter.aggregate(Stream.of(), SeriesAggregation.SUM)).isEqualTo(Optional.empty());
    }

    @Test
    public void testAggregate() {

        List<Boolean> input = new ArrayList<>();
        input.add(true);
        input.add(false);
        input.add(true);
        input.add(true);
        input.add(false);

        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.AVERAGE)).isEqualTo(Optional.of(true));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.COUNT)).isEqualTo(Optional.of(true));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.FIRST)).isEqualTo(Optional.of(true));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.LAST)).isEqualTo(Optional.of(false));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.MAXIMUM)).isEqualTo(Optional.of(true));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.MEDIAN)).isEqualTo(Optional.of(true));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.MINIMUM)).isEqualTo(Optional.of(false));
        assertThat(adapter.aggregate(input.stream(), SeriesAggregation.SUM)).isEqualTo(Optional.of(true));
    }

    @Test
    public void testCreate() throws Exception {
        File temp = File.createTempFile(BooleanFileBasedArrayTest.class.getSimpleName(), "");
        temp.deleteOnExit();
        temp.delete();
        try (FileBasedArray<Boolean> array = adapter.create(temp, 10)) {
            assertThat(array.size()).isEqualTo(10);
        }
        temp.delete();
    }

    @Test
    public void testRead() throws Exception {
        File temp = File.createTempFile(BooleanFileBasedArrayTest.class.getSimpleName(), "");
        temp.deleteOnExit();
        RandomAccessFile raf = new RandomAccessFile(temp, "rw");
        raf.setLength(10);
        raf.close();
        try (FileBasedArray<Boolean> array = adapter.read(temp, false)) {
            assertThat(array.size()).isEqualTo(10);
        }
        temp.delete();
    }
}
