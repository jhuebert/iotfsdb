package org.huebert.iotfsdb.service;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.time.LocalDateTime;
import java.util.Collection;

@Validated
@Service
public class InsertService {

    private final DataService dataService;

    private final PartitionService partitionService;

    public InsertService(@NotNull DataService dataService, @NotNull PartitionService partitionService) {
        this.dataService = dataService;
        this.partitionService = partitionService;
    }

    public void insert(@NotBlank String seriesId, @NotNull Collection<SeriesData> values) {
        PartitionPeriod partitionPeriod = dataService.getSeries(seriesId)
            .map(SeriesFile::getDefinition)
            .map(SeriesDefinition::getPartition)
            .orElseThrow();
        for (SeriesData value : values) {
            LocalDateTime local = TimeConverter.toUtc(value.getTime());
            PartitionKey key = PartitionKey.getKey(seriesId, partitionPeriod, local);
            PartitionRange details = partitionService.getRange(key);
            int index = details.getIndex(local);
            dataService.getBuffer(key, details.getSize(), details.adapter())
                .ifPresent(b -> details.adapter().put(b, index, value.getValue()));
        }
    }
}
