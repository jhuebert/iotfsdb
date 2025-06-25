package org.huebert.iotfsdb.service;

import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

@Validated
@Slf4j
@Service
public class CloneService {

    private final DataService dataService;

    private final PartitionService partitionService;

    private final SeriesService seriesService;

    private final InsertService insertService;

    public CloneService(DataService dataService, PartitionService partitionService, SeriesService seriesService, InsertService insertService) {
        this.dataService = dataService;
        this.partitionService = partitionService;
        this.seriesService = seriesService;
        this.insertService = insertService;
    }

    public void updateSeries(String id, SeriesFile updated) {
        log.info("Updating series for ID: {}", id);

        boolean idChanged = !id.equals(updated.getId());

        String sourceId = id;
        if (!idChanged) {
            // If the ID is staying the same, we need to move the series to a new ID first
            sourceId = UUID.randomUUID().toString();
            cloneSeries(id, sourceId);
            seriesService.deleteSeries(id);
        }

        seriesService.createSeries(updated);

        dataService.getPartitions(sourceId).stream()
            .map(this::convertPartition)
            .filter(values -> !values.isEmpty())
            .map(values -> new InsertRequest(updated.getId(), values, Reducer.FIRST))
            .forEach(insertService::insert);

        seriesService.deleteSeries(sourceId);

    }

    private List<SeriesData> convertPartition(PartitionKey partition) {
        PartitionRange range = partitionService.getRange(partition);
        List<SeriesData> result = new ArrayList<>();
        range.withRead(() -> {
            ByteBuffer buffer = dataService.getBuffer(partition).orElseThrow();
            ZonedDateTime current = TimeConverter.toUtc(range.getRange().lowerEndpoint());
            Iterator<Number> iterator = range.getStream(buffer).iterator();
            while (iterator.hasNext()) {
                result.add(new SeriesData(current, iterator.next()));
                current = current.plus(range.getInterval());
            }
        });
        return result;
    }

    public void cloneSeries(String sourceId, String destinationId) {
        log.debug("Cloning series from {} to {}", sourceId, destinationId);
        seriesService.createSeries(cloneSeriesFile(sourceId, destinationId));
        for (PartitionKey sourceKey : dataService.getPartitions(sourceId)) {
            clonePartition(sourceKey, destinationId);
        }
    }

    private SeriesFile cloneSeriesFile(String sourceId, String destinationId) {
        SeriesFile sourceSeries = seriesService.findSeries(sourceId).orElseThrow();
        return SeriesFile.builder()
            .definition(sourceSeries.getDefinition().toBuilder()
                .id(destinationId)
                .build())
            .metadata(new HashMap<>(sourceSeries.getMetadata()))
            .build();
    }

    private void clonePartition(PartitionKey sourceKey, String destinationId) {
        PartitionKey destinationKey = new PartitionKey(destinationId, sourceKey.partitionId());
        PartitionRange sourceRange = partitionService.getRange(sourceKey);
        PartitionRange destinationRange = partitionService.getRange(destinationKey);
        sourceRange.withRead(() -> {
            ByteBuffer sourceBuffer = dataService.getBuffer(sourceKey).orElseThrow();
            // Worried about deadlock
            destinationRange.withWrite(() -> {
                ByteBuffer destinationBuffer = dataService.getBuffer(destinationKey, sourceRange.getSize(), sourceRange.getAdapter());
                destinationBuffer.put(sourceBuffer);
            });
        });
    }

}
