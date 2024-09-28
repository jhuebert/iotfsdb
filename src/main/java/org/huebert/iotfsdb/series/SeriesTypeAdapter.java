package org.huebert.iotfsdb.series;

import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.schema.Series;

import java.io.File;
import java.time.LocalDateTime;
import java.util.stream.Stream;

public interface SeriesTypeAdapter<T> {

    FileBasedArray<T> readArray(File file, Series series, LocalDateTime start, boolean readOnly, boolean create);

    T aggregate(Stream<T> stream, SeriesAggregation aggregation);
}
