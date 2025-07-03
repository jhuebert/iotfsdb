package org.huebert.iotfsdb.partition;

import org.junit.jupiter.api.Test;

import java.nio.IntBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class BufferIteratorTest {

    @Test
    void testIterator() {
        BufferIterator<IntBuffer> iterator = new BufferIterator<>(IntBuffer.wrap(new int[]{1, 2}), 2, IntBuffer::get);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(1);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(2);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testGetStream() {
        BufferIterator<IntBuffer> iterator = new BufferIterator<>(IntBuffer.wrap(new int[]{1, 2}), 2, IntBuffer::get);
        assertThat(iterator.asStream().toList()).containsExactly(1, 2);
    }

}
