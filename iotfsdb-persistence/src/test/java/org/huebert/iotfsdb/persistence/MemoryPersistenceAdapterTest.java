package org.huebert.iotfsdb.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import org.huebert.iotfsdb.api.schema.PartitionKey;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.junit.jupiter.api.Test;

public class MemoryPersistenceAdapterTest {

    @Test
    public void testPostConstruct() {
        MemoryPersistenceAdapter adapter = new MemoryPersistenceAdapter();
        adapter.postConstruct();
    }

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
