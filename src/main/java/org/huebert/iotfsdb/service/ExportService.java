package org.huebert.iotfsdb.service;

import static org.huebert.iotfsdb.persistence.FilePersistenceAdapter.SERIES_JSON;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Validated
@Service
public class ExportService {

    private final DataService dataService;

    private final SeriesService seriesService;

    private final PartitionService partitionService;

    private final ObjectMapper objectMapper;

    public ExportService(@NotNull DataService dataService, @NotNull SeriesService seriesService, @NotNull PartitionService partitionService, @NotNull ObjectMapper objectMapper) {
        this.dataService = dataService;
        this.seriesService = seriesService;
        this.partitionService = partitionService;
        this.objectMapper = objectMapper;
    }

    public void export(@NotNull @Valid FindSeriesRequest request, @NotNull OutputStream outputStream) {
        try (ZipOutputStream zos = new ZipOutputStream(outputStream)) {
            zos.setLevel(Deflater.BEST_COMPRESSION);
            for (SeriesFile seriesFile : seriesService.findSeries(request)) {
                writeSeriesFileToZip(zos, seriesFile);
                writePartitionsToZip(zos, seriesFile.getId());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to export data", e);
        }
    }

    private void writeSeriesFileToZip(ZipOutputStream zos, SeriesFile seriesFile) throws IOException {
        addToZip(zos, seriesFile.getId(), SERIES_JSON, objectMapper.writeValueAsString(seriesFile).getBytes(StandardCharsets.UTF_8));
    }

    private void writePartitionsToZip(ZipOutputStream zos, String seriesId) throws IOException {
        for (PartitionKey key : dataService.getPartitions(seriesId)) {
            partitionService.getRange(key).withRead(() -> addToZip(zos, key.seriesId(), key.partitionId(), dataService.getBuffer(key).map(pbb -> pbb.withRead(ByteBuffer::array)).orElseThrow()));
        }
    }

    private void addToZip(ZipOutputStream zos, String seriesId, String filename, byte[] content) throws IOException {
        Path path = Path.of(seriesId, filename);
        ZipEntry entry = new ZipEntry(path.toString());
        zos.putNextEntry(entry);
        zos.write(content);
        zos.closeEntry();
    }

}
