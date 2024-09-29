package org.huebert.iotfsdb.file;

import java.util.List;

public interface FileBasedArray<T> extends AutoCloseable {

    int size();

    List<T> get(int start, int end);

    void set(int index, T value);

}
