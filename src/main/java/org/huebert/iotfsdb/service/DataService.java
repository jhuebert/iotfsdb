package org.huebert.iotfsdb.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.partition.PartitionAdapter;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Validated
@Slf4j
@Service
public class DataService {

    private final PersistenceAdapter persistenceAdapter;

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

    public synchronized void saveSeries(@Valid @NotNull SeriesFile seriesFile) {
        persistenceAdapter.saveSeries(seriesFile);
        seriesMap.put(seriesFile.getId(), seriesFile);
    }

    public synchronized void deleteSeries(@NotBlank String seriesId) {
        seriesMap.remove(seriesId);
        partitionCache.invalidateAll(seriesPartitions.remove(seriesId));
        persistenceAdapter.deleteSeries(seriesId);
    }

    public Set<PartitionKey> getPartitions(@NotBlank String seriesId) {
        return seriesPartitions.getOrDefault(seriesId, Set.of());
    }

    public Optional<ByteBuffer> getBuffer(@Valid @NotNull PartitionKey key) {
        if (partitionNotExists(key)) {
            return Optional.empty();
        }
        return Optional.of(partitionCache.getUnchecked(key).getByteBuffer());
    }

    public ByteBuffer getBuffer(@Valid @NotNull PartitionKey key, @NotNull @Positive Long size, @NotNull PartitionAdapter adapter) {
        if (partitionNotExists(key)) {
            synchronized (this) {
                if (partitionNotExists(key)) {

                    persistenceAdapter.createPartition(key, adapter.getTypeSize() * size);

                    PartitionByteBuffer partitionByteBuffer = persistenceAdapter.openPartition(key);
                    ByteBuffer byteBuffer = partitionByteBuffer.getByteBuffer();
                    for (int i = 0; i < size; i++) {
                        adapter.put(byteBuffer, i, null);
                    }
                    partitionByteBuffer.close();

                    seriesPartitions.computeIfAbsent(key.seriesId(), k -> ConcurrentHashMap.newKeySet()).add(key);
                }
            }
        }
        return partitionCache.getUnchecked(key).getByteBuffer();
    }

    private boolean partitionNotExists(PartitionKey key) {
        return !getPartitions(key.seriesId()).contains(key);
    }

    @Scheduled(fixedRate = 1, timeUnit = TimeUnit.MINUTES)
    public void cleanUp() {
        // Ensure that the cache is cleaned up periodically if there is no other activity
        partitionCache.cleanUp();
    }

}
