package org.huebert.iotfsdb.service;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.SeriesData;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        Map<PartitionKey, List<SeriesData>> partitionGroups = values.stream().collect(Collectors.groupingBy(value -> {
            LocalDateTime local = TimeConverter.toUtc(value.getTime());
            return PartitionKey.getKey(seriesId, partitionPeriod, local);
        }));
        partitionGroups.entrySet().parallelStream()
            .forEach(entry -> {
                PartitionKey key = entry.getKey();
                PartitionRange details = partitionService.getRange(key);
                details.withWrite(() -> {
                    ByteBuffer buffer = dataService.getBuffer(key, details.getSize(), details.adapter()).orElseThrow();
                    for (SeriesData value : entry.getValue()) {
                        LocalDateTime local = TimeConverter.toUtc(value.getTime());
                        int index = details.getIndex(local);
                        details.adapter().put(buffer, index, value.getValue());
                    }
                });
            });
    }
}
