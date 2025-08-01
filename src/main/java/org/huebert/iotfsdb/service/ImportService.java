package org.huebert.iotfsdb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.Reducer;
import org.huebert.iotfsdb.api.schema.SeriesData;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.persistence.FilePersistenceAdapter;
import org.huebert.iotfsdb.persistence.PartitionByteBuffer;
import org.huebert.iotfsdb.persistence.PersistenceAdapter;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Validated
@Service
public class ImportService {

    private final InsertService insertService;

    private final SeriesService seriesService;

    private final ObjectMapper objectMapper;

    public ImportService(@NotNull SeriesService seriesService, @NotNull InsertService insertService, @NotNull ObjectMapper objectMapper) {
        this.insertService = insertService;
        this.seriesService = seriesService;
        this.objectMapper = objectMapper;
    }

    public void importData(@NotNull Path path) {
        PersistenceAdapter adapter = FilePersistenceAdapter.create(path, objectMapper);
        try {

            for (SeriesFile series : adapter.getSeries()) {

                List<SeriesData> seriesData = adapter.getPartitions(series).stream()
                    .map(pk -> PartitionService.calculateRange(series.getDefinition(), pk))
                    .flatMap(pr -> convertPartition(adapter, pr).stream())
                    .toList();

                String seriesId = series.getId();
                if (seriesService.findSeries(seriesId).isEmpty()) {
                    seriesService.createSeries(series);
                }

                if (!seriesData.isEmpty()) {
                    insertService.insert(new InsertRequest(seriesId, seriesData, Reducer.FIRST));
                }

            }

        } finally {
            adapter.close();
        }
    }

    private static List<SeriesData> convertPartition(PersistenceAdapter adapter, PartitionRange partitionRange) {
        PartitionByteBuffer pbb = adapter.openPartition(partitionRange.getKey());
        try {
            List<SeriesData> result = new ArrayList<>();
            ZonedDateTime current = TimeConverter.toUtc(partitionRange.getRange().lowerEndpoint());
            Iterator<Number> iterator = partitionRange.getStream(pbb.getByteBuffer()).iterator();
            while (iterator.hasNext()) {
                Number value = iterator.next();
                if (value != null) {
                    result.add(new SeriesData(current, value));
                }
                current = current.plus(partitionRange.getInterval());
            }
            return result;
        } finally {
            pbb.close();
        }
    }

}
