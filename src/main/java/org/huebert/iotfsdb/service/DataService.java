package org.huebert.iotfsdb.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

@Validated
@Slf4j
@Service
public class DataService {

    private final PersistenceAdapter persistenceAdapter;

    private final ReentrantLock persistenceLock = new ReentrantLock();

    private final Map<String, SeriesFile> seriesMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Set<PartitionKey>> seriesPartitions = new ConcurrentHashMap<>();

    private final LoadingCache<PartitionKey, PartitionByteBuffer> partitionCache;

    public DataService(@NotNull IotfsdbProperties properties, @NotNull PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
        this.partitionCache = CacheBuilder.from(properties.getPartitionCache())
            .removalListener((RemovalListener<PartitionKey, PartitionByteBuffer>) notification -> {
                PartitionByteBuffer value = notification.getValue();
                if (value != null) {
                    value.close();
                }
            })
            .build(new CacheLoader<>(persistenceAdapter::openPartition));

        for (SeriesFile seriesFile : persistenceAdapter.getSeries()) {
            String seriesId = seriesFile.getId();
            seriesMap.put(seriesId, seriesFile);
            seriesPartitions.computeIfAbsent(seriesId, k -> ConcurrentHashMap.newKeySet()).addAll(persistenceAdapter.getPartitions(seriesFile));
        }
    }

    public List<SeriesFile> getSeries() {
        return List.copyOf(seriesMap.values());
    }

    public Optional<SeriesFile> getSeries(@NotBlank String seriesId) {
        return Optional.ofNullable(seriesMap.get(seriesId));
    }

    public void saveSeries(@Valid @NotNull SeriesFile seriesFile) {
        persistenceLock.lock();
        try {
            persistenceAdapter.saveSeries(seriesFile);
            seriesMap.put(seriesFile.getId(), seriesFile);
        } finally {
            persistenceLock.unlock();
        }
    }

    public void deleteSeries(@NotBlank String seriesId) {
        persistenceLock.lock();
        try {
            SeriesFile series = seriesMap.remove(seriesId);
            if (series == null) {
                throw new IllegalArgumentException(String.format("series (%s) does not exist", seriesId));
            }
            partitionCache.invalidateAll(seriesPartitions.remove(seriesId));
            persistenceAdapter.deleteSeries(seriesId);
        } finally {
            persistenceLock.unlock();
        }
    }

    public Set<PartitionKey> getPartitions(@NotBlank String seriesId) {
        return seriesPartitions.getOrDefault(seriesId, Set.of());
    }

    public Optional<ByteBuffer> getBuffer(@Valid @NotNull PartitionKey key) {
        return getBuffer(key, null, null);
    }

    public Optional<ByteBuffer> getBuffer(@Valid @NotNull PartitionKey key, Long size, PartitionAdapter adapter) {
        if (!seriesPartitions.containsKey(key.seriesId())) {

            if (adapter == null) {
                return Optional.empty();
            }

            persistenceLock.lock();
            try {

                if (!seriesPartitions.containsKey(key.seriesId())) {

                    persistenceAdapter.createPartition(key, adapter.getTypeSize() * size);

                    PartitionByteBuffer partitionByteBuffer = persistenceAdapter.openPartition(key);
                    ByteBuffer byteBuffer = partitionByteBuffer.getByteBuffer();
                    for (int i = 0; i < size; i++) {
                        adapter.put(byteBuffer, i, null);
                    }
                    partitionByteBuffer.close();

                    seriesPartitions.computeIfAbsent(key.seriesId(), k -> ConcurrentHashMap.newKeySet()).add(key);
                }

            } finally {
                persistenceLock.unlock();
            }
        }

        return Optional.of(partitionCache.getUnchecked(key))
            .map(PartitionByteBuffer::getByteBuffer);
    }

}
