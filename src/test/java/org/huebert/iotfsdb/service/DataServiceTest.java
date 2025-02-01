package org.huebert.iotfsdb.service;

import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataServiceTest {

    @Test
    public void testGet() {

        PersistenceAdapter persistenceAdapter = mock(PersistenceAdapter.class);

        SeriesFile seriesFile1 = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        SeriesFile seriesFile2 = SeriesFile.builder().definition(SeriesDefinition.builder().id("def").build()).build();

        when(persistenceAdapter.getSeries()).thenReturn(List.of(
            seriesFile1,
            seriesFile2
        ));

        PartitionKey seriesFile1Key1 = new PartitionKey("abc", "123");
        PartitionKey seriesFile1Key2 = new PartitionKey("abc", "456");

        when(persistenceAdapter.getPartitions(seriesFile1)).thenReturn(Set.of(
            seriesFile1Key1,
            seriesFile1Key2
        ));

        PartitionKey seriesFile2Key1 = new PartitionKey("def", "123");
        PartitionKey seriesFile2Key2 = new PartitionKey("def", "456");

        when(persistenceAdapter.getPartitions(seriesFile2)).thenReturn(Set.of(
            seriesFile2Key1,
            seriesFile2Key2
        ));

        DataService dataService = new DataService(new IotfsdbProperties(), persistenceAdapter);

        verify(persistenceAdapter).getSeries();
        verify(persistenceAdapter).getPartitions(seriesFile1);
        verify(persistenceAdapter).getPartitions(seriesFile2);

        List<SeriesFile> series = dataService.getSeries();
        assertThat(series).containsExactly(seriesFile1, seriesFile2);

        assertThat(dataService.getSeries("abc")).isEqualTo(Optional.of(seriesFile1));
        assertThat(dataService.getSeries("def")).isEqualTo(Optional.of(seriesFile2));
        assertThat(dataService.getSeries("ghi")).isEqualTo(Optional.empty());

        Set<PartitionKey> abcKeys = dataService.getPartitions("abc");
        assertThat(abcKeys.size()).isEqualTo(2);
        assertThat(abcKeys).contains(seriesFile1Key1, seriesFile1Key2);

        Set<PartitionKey> defKeys = dataService.getPartitions("def");
        assertThat(defKeys.size()).isEqualTo(2);
        assertThat(defKeys).contains(seriesFile2Key1, seriesFile2Key2);

        assertThat(dataService.getPartitions("ghi")).isEqualTo(Set.of());

        verify(persistenceAdapter).getSeries();
        verify(persistenceAdapter).getPartitions(seriesFile1);
        verify(persistenceAdapter).getPartitions(seriesFile2);
    }

    @Test
    public void testSaveSeries() {
        PersistenceAdapter persistenceAdapter = mock(PersistenceAdapter.class);
        when(persistenceAdapter.getSeries()).thenReturn(List.of());

        DataService dataService = new DataService(new IotfsdbProperties(), persistenceAdapter);
        verify(persistenceAdapter).getSeries();

        List<SeriesFile> series = dataService.getSeries();
        assertThat(series).isEmpty();

        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        dataService.saveSeries(seriesFile);

        verify(persistenceAdapter).saveSeries(seriesFile);
        assertThat(dataService.getSeries("abc")).isEqualTo(Optional.of(seriesFile));
    }

    @Test
    public void testDeleteSeries() {
        PersistenceAdapter persistenceAdapter = mock(PersistenceAdapter.class);

        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        when(persistenceAdapter.getSeries()).thenReturn(List.of(seriesFile));

        PartitionKey seriesFileKey1 = new PartitionKey("abc", "123");
        PartitionKey seriesFileKey2 = new PartitionKey("abc", "456");
        when(persistenceAdapter.getPartitions(seriesFile)).thenReturn(Set.of(
            seriesFileKey1,
            seriesFileKey2
        ));

        DataService dataService = new DataService(new IotfsdbProperties(), persistenceAdapter);
        verify(persistenceAdapter).getSeries();
        verify(persistenceAdapter).getPartitions(seriesFile);

        assertThat(dataService.getSeries()).containsExactly(seriesFile);
        assertThat(dataService.getSeries("abc")).isEqualTo(Optional.of(seriesFile));

        Set<PartitionKey> abcKeys = dataService.getPartitions("abc");
        assertThat(abcKeys.size()).isEqualTo(2);
        assertThat(abcKeys).contains(seriesFileKey1, seriesFileKey2);

        dataService.deleteSeries("abc");
        verify(persistenceAdapter).deleteSeries("abc");

        assertThat(dataService.getSeries()).isEmpty();
        assertThat(dataService.getSeries("abc")).isEqualTo(Optional.empty());
        assertThat(dataService.getPartitions("abc")).isEmpty();
    }

    @Test
    public void testDeleteSeries_NoPartitions() {
        PersistenceAdapter persistenceAdapter = mock(PersistenceAdapter.class);
        when(persistenceAdapter.getSeries()).thenReturn(List.of());

        DataService dataService = new DataService(new IotfsdbProperties(), persistenceAdapter);
        verify(persistenceAdapter).getSeries();

        dataService.deleteSeries("abc");
        verify(persistenceAdapter).deleteSeries("abc");

        assertThat(dataService.getSeries()).isEmpty();
        assertThat(dataService.getSeries("abc")).isEqualTo(Optional.empty());
        assertThat(dataService.getPartitions("abc")).isEmpty();
    }

    @Test
    public void testGetBuffer_NotExists() {
        PersistenceAdapter persistenceAdapter = mock(PersistenceAdapter.class);
        when(persistenceAdapter.getSeries()).thenReturn(List.of());

        DataService dataService = new DataService(new IotfsdbProperties(), persistenceAdapter);
        verify(persistenceAdapter).getSeries();

        List<SeriesFile> series = dataService.getSeries();
        assertThat(series).isEmpty();

        assertThat(dataService.getBuffer(new PartitionKey("abc", "123"))).isEqualTo(Optional.empty());
    }

    @Test
    public void testGetBuffer_NotExistsWithCreate() {
        PersistenceAdapter persistenceAdapter = mock(PersistenceAdapter.class);
        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        when(persistenceAdapter.getSeries()).thenReturn(List.of(seriesFile));

        DataService dataService = new DataService(new IotfsdbProperties(), persistenceAdapter);
        verify(persistenceAdapter).getSeries();

        List<SeriesFile> series = dataService.getSeries();
        assertThat(series).isEqualTo(List.of(seriesFile));

        PartitionKey key = new PartitionKey("abc", "123");

        PartitionAdapter adapter = mock(PartitionAdapter.class);
        when(adapter.getTypeSize()).thenReturn(4);

        PartitionByteBuffer partitionByteBuffer = mock(PartitionByteBuffer.class);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        when(partitionByteBuffer.getByteBuffer()).thenReturn(byteBuffer);

        when(persistenceAdapter.openPartition(key)).thenReturn(partitionByteBuffer);

        assertThat(dataService.getBuffer(key, 2L, adapter)).isEqualTo(byteBuffer);

        verify(persistenceAdapter).createPartition(key, 8L);
    }

    @Test
    public void testGetBuffer() {
        PersistenceAdapter persistenceAdapter = mock(PersistenceAdapter.class);

        SeriesFile seriesFile = SeriesFile.builder().definition(SeriesDefinition.builder().id("abc").build()).build();
        when(persistenceAdapter.getSeries()).thenReturn(List.of(seriesFile));

        PartitionKey seriesFileKey = new PartitionKey("abc", "123");
        when(persistenceAdapter.getPartitions(seriesFile)).thenReturn(Set.of(seriesFileKey));

        DataService dataService = new DataService(new IotfsdbProperties(), persistenceAdapter);
        verify(persistenceAdapter).getSeries();
        verify(persistenceAdapter).getPartitions(seriesFile);

        PartitionByteBuffer partitionByteBuffer = mock(PartitionByteBuffer.class);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        when(partitionByteBuffer.getByteBuffer()).thenReturn(byteBuffer);
        when(persistenceAdapter.openPartition(seriesFileKey)).thenReturn(partitionByteBuffer);

        assertThat(dataService.getBuffer(seriesFileKey)).isEqualTo(Optional.of(byteBuffer));
        dataService.deleteSeries("abc");
        verify(partitionByteBuffer).close();
    }

    @Test
    public void testCleanUp() {
        PersistenceAdapter persistenceAdapter = mock(PersistenceAdapter.class);
        when(persistenceAdapter.getSeries()).thenReturn(List.of());
        DataService dataService = new DataService(new IotfsdbProperties(), persistenceAdapter);
        dataService.cleanUp();
    }

}
