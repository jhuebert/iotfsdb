package org.huebert.iotfsdb.persistence;

import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.PartitionKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MemoryPersistenceAdapterTest {

    @Test
    public void testSeries() {
        MemoryPersistenceAdapter adapter = new MemoryPersistenceAdapter();
        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("123").build()).build();
        assertThat(adapter.getSeries()).isEmpty();
        adapter.saveSeries(seriesFile);
        assertThat(adapter.getSeries()).containsExactly(seriesFile);
        adapter.deleteSeries(seriesFile.getId());
        assertThat(adapter.getSeries()).isEmpty();
        adapter.close();
    }

    @Test
    public void testPartition() {
        MemoryPersistenceAdapter adapter = new MemoryPersistenceAdapter();
        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("123").build()).build();
        PartitionKey key = new PartitionKey("123", "456");
        adapter.saveSeries(seriesFile);
        assertThat(adapter.getPartitions(seriesFile)).isEmpty();
        adapter.createPartition(key, 80);
        PartitionByteBuffer partitionByteBuffer = adapter.openPartition(key);
        assertThat(partitionByteBuffer.getByteBuffer().capacity()).isEqualTo(80);
        partitionByteBuffer.close();
        adapter.close();
    }

}
