package org.huebert.iotfsdb.partition;

import java.nio.Buffer;
import java.util.Iterator;
import java.util.function.Function;

public class BufferIterator<T extends Buffer> implements Iterator<Number> {

    private final T buffer;

    private final Function<T, Number> function;

    public BufferIterator(T buffer, Function<T, Number> function) {
        this.buffer = buffer;
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

}
