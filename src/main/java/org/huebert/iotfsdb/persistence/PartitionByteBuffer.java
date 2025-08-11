package org.huebert.iotfsdb.persistence;

import lombok.Builder;
import lombok.Data;
import org.huebert.iotfsdb.service.LockUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

@Builder
public class PartitionByteBuffer {

    private final FileChannel fileChannel;

    private final ByteBuffer byteBuffer;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public <T> T withRead(Function<ByteBuffer, T> function) {
        Result<T> result = new Result<>();
        LockUtil.withRead(lock, () -> result.setValue(function.apply(byteBuffer)));
        return result.getValue();
    }

    public void withWrite(Consumer<ByteBuffer> consumer) {
        LockUtil.withWrite(lock, () -> consumer.accept(byteBuffer));
    }

    public void close() {
        if (fileChannel != null) {
            LockUtil.withWrite(lock, () -> {
                try {
                    fileChannel.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Data
    private static class Result<T> {
        private T value;
    }

}
