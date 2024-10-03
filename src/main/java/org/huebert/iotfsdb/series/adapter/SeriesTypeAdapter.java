package org.huebert.iotfsdb.series.adapter;

import org.huebert.iotfsdb.file.FileBasedArray;
import org.huebert.iotfsdb.series.SeriesAggregation;

import java.io.File;
import java.util.Optional;
import java.util.stream.Stream;

public interface SeriesTypeAdapter<T> {

    FileBasedArray<T> create(File file, int size);

    FileBasedArray<T> read(File file, boolean readOnly);

    Optional<T> aggregate(Stream<T> stream, SeriesAggregation aggregation);

    T convert(String value);

}
