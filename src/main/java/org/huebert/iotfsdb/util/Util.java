package org.huebert.iotfsdb.util;

import com.google.common.collect.Range;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class Util {

    public static File checkFile(File file) {

        if (file == null) {
            throw new IllegalArgumentException("file is null");
        }

        if (!file.exists()) {
            throw new IllegalArgumentException(String.format("file (%s) does not exist", file));
        }

        if (!file.isFile()) {
            throw new IllegalArgumentException(String.format("path (%s) is not a file", file));
        }

        if (!file.canRead()) {
            throw new IllegalArgumentException(String.format("unable to read file (%s)", file));
        }

        return file;
    }

    public static File checkFileWrite(File file) {
        if (!file.canWrite()) {
            throw new IllegalArgumentException(String.format("unable to write file (%s)", file));
        }
        return checkFile(file);
    }

    public static File checkDirectory(File dir) {

        if (dir == null) {
            throw new IllegalArgumentException("dir is null");
        }

        if (!dir.exists()) {
            throw new IllegalArgumentException(String.format("directory (%s) does not exist", dir));
        }

        if (!dir.isDirectory()) {
            throw new IllegalArgumentException(String.format("path (%s) is not a directory", dir));
        }

        if (!dir.canRead()) {
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
