package org.huebert.iotfsdb.partition;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface PartitionAdapter {

    int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.SIZED | Spliterator.IMMUTABLE | Spliterator.SUBSIZED;

    int getTypeSize();

    void put(ByteBuffer buffer, int index, Number value);

    Iterator<Number> getIterator(ByteBuffer buffer, int index, int length);

    default Stream<Number> getStream(ByteBuffer buffer, int index, int length) {
        Iterator<Number> iterator = getIterator(buffer, index, length);
        Spliterator<Number> spliterator = Spliterators.spliterator(iterator, length, CHARACTERISTICS);
        return StreamSupport.stream(spliterator, false);
    }

}
