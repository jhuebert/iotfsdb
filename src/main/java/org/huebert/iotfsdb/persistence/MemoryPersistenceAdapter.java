package org.huebert.iotfsdb.persistence;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.PartitionKey;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Validated
@Service
@ConditionalOnExpression("'${iotfsdb.persistence.root:}' == 'memory'")
public class MemoryPersistenceAdapter implements PersistenceAdapter {

    private final ConcurrentMap<String, SeriesFile> seriesMap = new ConcurrentHashMap<>();

    private final SetMultimap<String, PartitionKey> partitionMap = HashMultimap.create();

    private final ConcurrentMap<PartitionKey, MemoryPartitionByteBuffer> byteBufferMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void postConstruct() {
        log.info("Using {}", getClass().getSimpleName());
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
    }

    @Override
    public PartitionByteBuffer openPartition(@NotNull @Valid PartitionKey key) {
        return byteBufferMap.get(key);
    }

    @Override
    public void close() {
        // Do nothing
    }

    @AllArgsConstructor
    private static class MemoryPartitionByteBuffer implements PartitionByteBuffer {

        private final ByteBuffer byteBuffer;

        @Override
        public ByteBuffer getByteBuffer() {
            return byteBuffer.slice();
        }

        @Override
        public void close() {
            // Do nothing
        }
    }

}
