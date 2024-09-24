package org.huebert.iotfsdb.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileUtil {

    public static Stream<Path> list(Path path) {
        try {
            return Files.list(path);
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

}
