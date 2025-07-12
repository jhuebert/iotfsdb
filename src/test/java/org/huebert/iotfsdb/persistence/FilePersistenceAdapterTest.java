package org.huebert.iotfsdb.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.PartitionKey;
import org.junit.jupiter.api.Test;
import org.springframework.util.FileSystemUtils;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilePersistenceAdapterTest {

    @Test
    void testNullRoot() {
        IotfsdbProperties properties = new IotfsdbProperties();
        properties.getPersistence().setRoot(null);
        assertThrows(IllegalArgumentException.class, () -> new FilePersistenceAdapter(properties, new ObjectMapper()));
    }

    @Test
    void testNotExists() throws Exception {

        Path temp = Files.createTempDirectory("iotfsdb");

        if (!FileSystemUtils.deleteRecursively(temp)) {
            throw new RuntimeException("unable to delete root");
        }

        IotfsdbProperties properties = new IotfsdbProperties();
        properties.getPersistence().setRoot(temp);

        assertThat(Files.exists(temp)).isFalse();

        FilePersistenceAdapter adapter = new FilePersistenceAdapter(properties, new ObjectMapper());

        assertThat(Files.exists(temp)).isTrue();

        adapter.close();
        if (!FileSystemUtils.deleteRecursively(temp)) {
            throw new RuntimeException("unable to delete root");
        }
    }

    @Test
    void testCreate() throws Exception {

        Path temp = Files.createTempDirectory("iotfsdb");
        IotfsdbProperties properties = new IotfsdbProperties();
        properties.getPersistence().setRoot(temp);

        ObjectMapper objectMapper = new ObjectMapper();
        FilePersistenceAdapter adapter = FilePersistenceAdapter.create(properties.getPersistence().getRoot(), objectMapper);

        assertThat(adapter.getSeries()).isEqualTo(List.of());

        adapter.close();
        if (!FileSystemUtils.deleteRecursively(temp)) {
            throw new RuntimeException("unable to delete root");
        }
    }

    @Test
    void testGetSeries() throws Exception {

        Path temp = Files.createTempDirectory("iotfsdb");
        IotfsdbProperties properties = new IotfsdbProperties();
        properties.getPersistence().setRoot(temp);

        ObjectMapper objectMapper = new ObjectMapper();
        FilePersistenceAdapter adapter = new FilePersistenceAdapter(properties, objectMapper);

        assertThat(adapter.getSeries()).isEqualTo(List.of());

        Path seriesRoot = temp.resolve("abc123");
        Files.createDirectories(seriesRoot);

        assertThat(adapter.getSeries()).isEqualTo(List.of());

        Path somefile = seriesRoot.resolve("somefile");
        Files.createFile(somefile);

        assertThat(adapter.getSeries()).isEqualTo(List.of());

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("abc123")
                .type(NumberType.FLOAT4)
                .interval(60000L)
                .partition(PartitionPeriod.MONTH)
                .build())
            .metadata(Map.of("a", "1", "b", "2"))
            .build();
        Path seriesJson = seriesRoot.resolve("series.json");
        objectMapper.writeValue(seriesJson.toFile(), seriesFile);

        assertThat(adapter.getSeries()).isEqualTo(List.of(seriesFile));

        adapter.close();
        if (!FileSystemUtils.deleteRecursively(temp)) {
            throw new RuntimeException("unable to delete root");
        }
    }

    @Test
    void testSaveAndDeleteSeries() throws Exception {

        Path temp = Files.createTempDirectory("iotfsdb");
        IotfsdbProperties properties = new IotfsdbProperties();
        properties.getPersistence().setRoot(temp);

        ObjectMapper objectMapper = new ObjectMapper();
        FilePersistenceAdapter adapter = new FilePersistenceAdapter(properties, objectMapper);

        assertThat(adapter.getSeries()).isEqualTo(List.of());

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("abc123")
                .type(NumberType.FLOAT4)
                .interval(60000L)
                .partition(PartitionPeriod.MONTH)
                .build())
            .metadata(Map.of("a", "1", "b", "2"))
            .build();

        adapter.saveSeries(seriesFile);

        assertThat(adapter.getSeries()).isEqualTo(List.of(seriesFile));

        adapter.deleteSeries(seriesFile.getId());

        assertThat(adapter.getSeries()).isEqualTo(List.of());

        adapter.close();
        if (!FileSystemUtils.deleteRecursively(temp)) {
            throw new RuntimeException("unable to delete root");
        }
    }

    @Test
    void testPartition() throws Exception {

        Path temp = Files.createTempDirectory("iotfsdb");
        IotfsdbProperties properties = new IotfsdbProperties();
        properties.getPersistence().setRoot(temp);

        ObjectMapper objectMapper = new ObjectMapper();
        FilePersistenceAdapter adapter = new FilePersistenceAdapter(properties, objectMapper);

        assertThat(adapter.getSeries()).isEqualTo(List.of());

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("abc123")
                .type(NumberType.FLOAT4)
                .interval(60000L)
                .partition(PartitionPeriod.MONTH)
                .build())
            .metadata(Map.of("a", "1", "b", "2"))
            .build();

        adapter.saveSeries(seriesFile);

        assertThat(adapter.getSeries()).isEqualTo(List.of(seriesFile));

        Path somefile = temp.resolve("abc123").resolve("somefile");
        Files.createFile(somefile);

        assertThat(adapter.getPartitions(seriesFile)).isEqualTo(Set.of());

        PartitionKey key = new PartitionKey(seriesFile.getId(), "202411");
        adapter.createPartition(key, 80);

        assertThat(adapter.getPartitions(seriesFile)).isEqualTo(Set.of(key));

        PartitionByteBuffer partitionByteBuffer = adapter.openPartition(key);
        ByteBuffer byteBuffer = partitionByteBuffer.getByteBuffer();
        byteBuffer.asFloatBuffer().put(0, 4.2f);
        assertThat(byteBuffer.capacity()).isEqualTo(80);
        partitionByteBuffer.close();

        adapter.createPartition(key, 2000);

        partitionByteBuffer = adapter.openPartition(key);
        byteBuffer = partitionByteBuffer.getByteBuffer();
        byteBuffer.asFloatBuffer().put(0, 4.2f);
        assertThat(byteBuffer.capacity()).isEqualTo(80);
        partitionByteBuffer.close();

        adapter.close();
        if (!FileSystemUtils.deleteRecursively(temp)) {
            throw new RuntimeException("unable to delete root");
        }
    }

    @Test
    void testReadZip() throws Exception {

        Path temp = Files.createTempFile("iotfsdb", ".zip");
        try (InputStream is = FilePersistenceAdapterTest.class.getResourceAsStream("/db.zip"); OutputStream os = new FileOutputStream(temp.toFile())) {
            is.transferTo(os);
        }

        IotfsdbProperties properties = new IotfsdbProperties();
        properties.getPersistence().setRoot(temp);

        ObjectMapper objectMapper = new ObjectMapper();
        FilePersistenceAdapter adapter = new FilePersistenceAdapter(properties, objectMapper);

        SeriesFile seriesFile = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .id("abc123")
                .type(NumberType.FLOAT4)
                .interval(60000L)
                .partition(PartitionPeriod.MONTH)
                .build())
            .metadata(Map.of("a", "1", "b", "2"))
            .build();

        assertThat(adapter.getSeries()).isEqualTo(List.of(seriesFile));

        PartitionKey key = new PartitionKey(seriesFile.getId(), "202411");

        assertThat(adapter.getPartitions(seriesFile)).isEqualTo(Set.of(key));

        PartitionByteBuffer partitionByteBuffer = adapter.openPartition(key);
        ByteBuffer byteBuffer = partitionByteBuffer.getByteBuffer();
        assertThat(byteBuffer.capacity()).isEqualTo(80);
        partitionByteBuffer.close();

        adapter.close();
        if (!FileSystemUtils.deleteRecursively(temp)) {
            throw new RuntimeException("unable to delete root");
        }
    }
}
