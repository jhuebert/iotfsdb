package org.huebert.iotfsdb.service;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.schema.FindSeriesRequest;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@Validated
@Slf4j
@Service
public class SeriesService {

    private final DataService dataService;

    public SeriesService(@NotNull DataService dataService) {
        this.dataService = dataService;
    }

    public void createSeries(@Valid @NotNull SeriesFile seriesFile) {
        if (dataService.getSeries(seriesFile.getId()).isPresent()) {
            throw new IllegalArgumentException(String.format("series (%s) already exists", seriesFile.getId()));
        }
        dataService.saveSeries(seriesFile);
    }

    public Optional<SeriesFile> findSeries(@NotBlank String seriesId) {
        return dataService.getSeries(seriesId);
    }

    public List<SeriesFile> findSeries(@Valid @NotNull FindSeriesRequest request) {
        return dataService.getSeries().parallelStream()
            .filter(s -> request.getPattern().matcher(s.getId()).matches())
            .filter(s -> matchesMetadata(s, request.getMetadata()))
            .sorted(Comparator.comparing(SeriesFile::getId))
            .toList();
    }

    public void updateMetadata(@NotBlank String seriesId, @NotNull Map<String, String> metadata) {
        SeriesFile seriesFile = dataService.getSeries(seriesId).orElseThrow();
        dataService.saveSeries(new SeriesFile(seriesFile.getDefinition(), metadata));
    }

    public void deleteSeries(@NotBlank String seriesId) {
        dataService.deleteSeries(seriesId);
    }

    private boolean matchesMetadata(@Valid @NotNull SeriesFile series, @NotNull Map<String, Pattern> metadata) {
        Map<String, String> seriesMetadata = series.getMetadata();

        if (metadata.isEmpty()) {
            return true;
        } else if (metadata.size() > seriesMetadata.size()) {
            return false;
        }

        for (Map.Entry<String, Pattern> entry : metadata.entrySet()) {
            String seriesValue = seriesMetadata.get(entry.getKey());
            if (!entry.getValue().matcher(seriesValue).matches()) {
                return false;
            }
        }
        return true;
    }

}
