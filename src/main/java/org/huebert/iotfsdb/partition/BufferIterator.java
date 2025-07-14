package org.huebert.iotfsdb.partition;

import java.nio.Buffer;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class BufferIterator<T extends Buffer> implements Iterator<Number> {

    private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.SIZED | Spliterator.IMMUTABLE | Spliterator.SUBSIZED;

    private final T buffer;

    private final int length;

    private final Function<T, Number> function;

    public BufferIterator(T buffer, int length, Function<T, Number> function) {
        this.buffer = buffer;
        this.length = length;
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        return buffer.hasRemaining();
    }

    @Override
    public Number next() {
        return function.apply(buffer);
    }

    public Stream<Number> asStream() {
        return StreamSupport.stream(Spliterators.spliterator(this, length, CHARACTERISTICS), false);
    }

}
