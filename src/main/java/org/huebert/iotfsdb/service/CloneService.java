package org.huebert.iotfsdb.service;

import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.util.HashMap;

@Validated
@Slf4j
@Service
public class CloneService {

    private final DataService dataService;

    private final PartitionService partitionService;

    private final SeriesService seriesService;

    public CloneService(DataService dataService, PartitionService partitionService, SeriesService seriesService) {
        this.dataService = dataService;
        this.partitionService = partitionService;
        this.seriesService = seriesService;
    }

    public void clone(String sourceId, String destinationId) {
        log.debug("Cloning series from {} to {}", sourceId, destinationId);
        SeriesFile sourceSeries = seriesService.findSeries(sourceId).orElseThrow();
        dataService.saveSeries(cloneDefinition(sourceSeries, destinationId));
        for (PartitionKey sourceKey : dataService.getPartitions(sourceId)) {
            clonePartition(sourceKey, destinationId);
        }
    }

    private static SeriesFile cloneDefinition(SeriesFile sourceSeries, String destinationId) {
        return SeriesFile.builder()
            .definition(sourceSeries.getDefinition().toBuilder()
                .id(destinationId)
                .build())
            .metadata(new HashMap<>(sourceSeries.getMetadata()))
            .build();
    }

    private void clonePartition(PartitionKey sourceKey, String destinationId) {
        ByteBuffer sourceBuffer = dataService.getBuffer(sourceKey).orElseThrow();
        PartitionRange sourceRange = partitionService.getRange(sourceKey);
        PartitionKey destinationKey = new PartitionKey(destinationId, sourceKey.partitionId());
        ByteBuffer destinationBuffer = dataService.getBuffer(destinationKey, sourceRange.getSize(), sourceRange.getAdapter());
        destinationBuffer.put(sourceBuffer);
    }

}
