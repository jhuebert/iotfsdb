package org.huebert.iotfsdb.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.properties.IotfsdbProperties;
import org.huebert.iotfsdb.schema.PartitionPeriod;
import org.huebert.iotfsdb.schema.SeriesDefinition;
import org.huebert.iotfsdb.schema.SeriesFile;
import org.huebert.iotfsdb.service.PartitionKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;
import org.springframework.validation.annotation.Validated;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

@Slf4j
@Validated
@Service
@ConditionalOnExpression("'${iotfsdb.root:}' != 'memory'")
public class FilePersistenceAdapter implements PersistenceAdapter {

    private static final OpenOption[] OPEN_OPTIONS_READ = new OpenOption[]{StandardOpenOption.READ};

    private static final OpenOption[] OPEN_OPTIONS_TEMP = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE};

    private static final OpenOption[] OPEN_OPTIONS_READ_WRITE = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC};

    public static final String SERIES_JSON = "series.json";

    private final Path propertyRoot;

    private final ObjectMapper objectMapper;

    private final FileSystem fileSystem;

    private final Path rootPath;

    private final boolean zip;

    @Autowired
    public FilePersistenceAdapter(@NotNull IotfsdbProperties properties, @NotNull ObjectMapper objectMapper) {
        this(properties.getRoot(), objectMapper);
    }

    public static FilePersistenceAdapter create(Path propertyRoot, ObjectMapper objectMapper) {
        return new FilePersistenceAdapter(propertyRoot, objectMapper);
    }

    private FilePersistenceAdapter(Path propertyRoot, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.propertyRoot = propertyRoot;

        if (propertyRoot == null) {
            throw new IllegalArgumentException("root database path is null");
        }

        if (!Files.exists(propertyRoot)) {
            try {
                Files.createDirectories(propertyRoot);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (Files.isDirectory(propertyRoot)) {
            zip = false;
            fileSystem = null;
            rootPath = propertyRoot;
        } else {
            try {
                zip = true;
                fileSystem = FileSystems.newFileSystem(propertyRoot, Map.of());
                rootPath = fileSystem.getPath("/");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @PostConstruct
    public void postConstruct() {
        log.info("Using {}", FilePersistenceAdapter.class.getSimpleName());
    }

    @Override
    public List<SeriesFile> getSeries() {
        try (Stream<Path> stream = Files.list(rootPath)) {
            return stream
                .filter(Files::isDirectory)
                .filter(Files::isReadable)
                .map(s -> s.resolve(SERIES_JSON))
                .filter(Files::exists)
                .filter(Files::isRegularFile)
                .filter(Files::isReadable)
                .map(file -> {
                    try {
                        return objectMapper.readValue(file.toUri().toURL(), SeriesFile.class);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveSeries(@NotNull @Valid SeriesFile seriesFile) {
        Preconditions.checkArgument(!zip);
        try {
            Files.createDirectories(getSeriesRoot(seriesFile.getId()));
            objectMapper.writeValue(getSeriesFilePath(seriesFile.getId()).toFile(), seriesFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteSeries(@NotBlank String seriesId) {
        Preconditions.checkArgument(!zip);
        if (!FileSystemUtils.deleteRecursively(getSeriesRoot(seriesId).toFile())) {
            throw new RuntimeException(String.format("unable to delete series (%s)", seriesId));
        }
    }

    @Override
    public Set<PartitionKey> getPartitions(@NotNull @Valid SeriesFile seriesFile) {
        PartitionPeriod partitionPeriod = seriesFile.getDefinition().getPartition();
        String seriesId = seriesFile.getId();
        try (Stream<Path> stream = Files.list(getSeriesRoot(seriesId))) {
            return stream
                .filter(Files::exists)
                .filter(Files::isRegularFile)
                .filter(Files::isReadable)
                .map(Path::getFileName)
                .map(Path::toString)
                .filter(partitionPeriod::matches)
                .map(partitionId -> new PartitionKey(seriesId, partitionId))
                .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createPartition(@NotNull @Valid PartitionKey key, @Positive long size) {
        Preconditions.checkArgument(!zip);
        Path path = getPartitionPath(key);
        if (Files.exists(path)) {
            log.debug("Partition {} already exists", path);
            return;
        }
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PartitionByteBuffer openPartition(@NotNull @Valid PartitionKey key) {
        Path path = getPartitionPath(key);
        boolean readOnly = zip || !Files.isWritable(path);
        try {
            OpenOption[] openOptions = readOnly ? OPEN_OPTIONS_READ : OPEN_OPTIONS_READ_WRITE;
            if (zip) {
                String prefix = String.format("iotfsdb-%s-%s-", key.seriesId(), key.partitionId());
                path = Files.copy(path, Files.createTempFile(propertyRoot.getParent(), prefix, ".tmp"), StandardCopyOption.REPLACE_EXISTING);
                openOptions = OPEN_OPTIONS_TEMP;
            }
            // File size must be retrieved before open if it is a temp file that deletes on close
            long fileSize = Files.size(path);
            FileChannel fileChannel = FileChannel.open(path, openOptions);
            MappedByteBuffer byteBuffer = fileChannel.map(readOnly ? READ_ONLY : READ_WRITE, 0, fileSize);
            return new FileByteBuffer(fileChannel, byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Path getSeriesRoot(String seriesId) {
        if (!seriesId.matches(SeriesDefinition.ID_PATTERN)) {
            throw new IllegalArgumentException("series ID is malformed");
        }
        return rootPath.resolve(seriesId);
    }

    private Path getSeriesFilePath(String seriesId) {
        return getSeriesRoot(seriesId).resolve(SERIES_JSON);
    }

    private Path getPartitionPath(PartitionKey key) {
        return getSeriesRoot(key.seriesId()).resolve(key.partitionId());
    }

    @AllArgsConstructor
    private static class FileByteBuffer implements PartitionByteBuffer {

        private final FileChannel fileChannel;

        private final MappedByteBuffer byteBuffer;

        @Override
        public ByteBuffer getByteBuffer() {
            return byteBuffer.slice(0, byteBuffer.capacity());
        }

        @Override
        public void close() {
            try {
                fileChannel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
