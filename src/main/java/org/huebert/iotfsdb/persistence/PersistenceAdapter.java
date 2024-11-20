package org.huebert.iotfsdb.persistence;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.PartitionKey;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Set;

@Validated
public interface PersistenceAdapter {

    List<SeriesFile> getSeries();

    void saveSeries(@NotNull @Valid SeriesFile seriesFile);

    void deleteSeries(@NotBlank String seriesId);

    Set<PartitionKey> getPartitions(@NotNull @Valid SeriesFile seriesFile);

    void createPartition(@NotNull @Valid PartitionKey key, @Positive long size);

    PartitionByteBuffer openPartition(@NotNull @Valid PartitionKey key);

}
