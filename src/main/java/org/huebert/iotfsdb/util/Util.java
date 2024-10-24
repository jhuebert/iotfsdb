package org.huebert.iotfsdb.util;

import com.google.common.collect.Range;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class Util {

    public static ZonedDateTime min(ZonedDateTime a, ZonedDateTime b) {
        if ((a == null) || (b == null)) {
            if (a != null) {
                return a;
            }
            return b;
        }
        return a.compareTo(b) <= 0 ? a : b;
    }

    public static ZonedDateTime max(ZonedDateTime a, ZonedDateTime b) {
        if ((a == null) || (b == null)) {
            if (a != null) {
                return a;
            }
            return b;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    public static Path unzipToTempFile(Path archiveZip, String path) {
        try (ZipFile zipFile = new ZipFile(archiveZip.toFile())) {
            ZipEntry entry = zipFile.getEntry(path);
            try (InputStream inputStream = zipFile.getInputStream(entry)) {
                String series = archiveZip.getParent().getFileName().toString();
                String prefix = String.join("-", "iotfsdb", series, path);
                Path tempFile = Files.createTempFile(prefix, "");
                Files.copy(inputStream, tempFile);
                return tempFile;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Path> list(Path path) {
        try {
            return Files.list(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path createDirectories(Path path) {
        try {
            return Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path copy(Path from, Path to) {
        try {
            return Files.copy(from, to);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path checkFile(Path path) {

        if (path == null) {
            throw new IllegalArgumentException("file is null");
        }

        if (!Files.exists(path)) {
            throw new IllegalArgumentException(String.format("file (%s) does not exist", path));
        }

        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException(String.format("path (%s) is not a file", path));
        }

        if (!Files.isReadable(path)) {
            throw new IllegalArgumentException(String.format("unable to read file (%s)", path));
        }

        return path;
    }

    public static Path checkFileWrite(Path path) {
        if (!Files.isWritable(path)) {
            throw new IllegalArgumentException(String.format("unable to write file (%s)", path));
        }
        return checkFile(path);
    }

    public static Path checkDirectory(Path dir) {

        if (dir == null) {
            throw new IllegalArgumentException("dir is null");
        }

        if (!Files.exists(dir)) {
            throw new IllegalArgumentException(String.format("directory (%s) does not exist", dir));
        }

        if (!Files.isDirectory(dir)) {
            throw new IllegalArgumentException(String.format("path (%s) is not a directory", dir));
        }

        if (!Files.isReadable(dir)) {
            throw new IllegalArgumentException(String.format("unable to read directory (%s)", dir));
        }

        return dir;
    }

    public static Range<LocalDateTime> convertToUtc(Range<ZonedDateTime> zonedRange) {
        return Range.range(
            convertToUtc(zonedRange.lowerEndpoint()),
            zonedRange.lowerBoundType(),
            convertToUtc(zonedRange.upperEndpoint()),
            zonedRange.upperBoundType()
        );
    }

    public static LocalDateTime convertToUtc(ZonedDateTime zonedDateTime) {
        return zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

}
