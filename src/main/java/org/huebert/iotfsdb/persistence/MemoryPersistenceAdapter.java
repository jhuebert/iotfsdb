package org.huebert.iotfsdb.persistence;

import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.PartitionKey;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Validated
@Service
@ConditionalOnProperty(prefix = "iotfsdb", value = "root", havingValue = "memory", matchIfMissing = true)
public class MemoryPersistenceAdapter implements PersistenceAdapter {

    private final Map<String, SeriesFile> seriesMap = new HashMap<>();

    private final Map<String, Set<PartitionKey>> partitionMap = new HashMap<>();

    private final Map<PartitionKey, MemoryPartitionByteBuffer> byteBufferMap = new HashMap<>();

    @PostConstruct
    public void postConstruct() {
        log.info("Using {}", MemoryPersistenceAdapter.class.getSimpleName());
    }

    @Override
    public List<SeriesFile> getSeries() {
        return List.copyOf(seriesMap.values());
    }

    @Override
    public void saveSeries(@NotNull @Valid SeriesFile seriesFile) {
        seriesMap.put(seriesFile.getId(), seriesFile);
    }

    @Override
    public void deleteSeries(@NotBlank String seriesId) {
        seriesMap.remove(seriesId);
    }

    @Override
    public Set<PartitionKey> getPartitions(@NotNull @Valid SeriesFile seriesFile) {
        return partitionMap.get(seriesFile.getId());
    }

    @Override
    public void createPartition(@NotNull @Valid PartitionKey key, @Positive long size) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int) size);
        byteBufferMap.put(key, new MemoryPartitionByteBuffer(byteBuffer));
        partitionMap.computeIfAbsent(key.seriesId(), k -> new HashSet<>()).add(key);
    }

    @Override
    public PartitionByteBuffer openPartition(@NotNull @Valid PartitionKey key) {
        return byteBufferMap.get(key);
    }

    @AllArgsConstructor
    private static class MemoryPartitionByteBuffer implements PartitionByteBuffer {

        private final ByteBuffer byteBuffer;

        @Override
        public ByteBuffer getByteBuffer() {
            return byteBuffer.slice(0, byteBuffer.capacity());
        }

        @Override
        public void close() {
            // Do nothing
        }
    }

}
