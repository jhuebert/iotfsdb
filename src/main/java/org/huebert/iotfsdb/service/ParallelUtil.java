package org.huebert.iotfsdb.service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

public class ParallelUtil {

    public static <T> void forEach(Collection<T> collection, Consumer<T> consumer) {
        map(collection, item -> {
            consumer.accept(item);
            return null;
        });
    }

    public static <I, O> List<O> map(Collection<I> collection, Function<I, O> mapper) {
        try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {
            return collection.stream()
                .map(item -> CompletableFuture.supplyAsync(() -> mapper.apply(item), es))
                .toList()
                .stream()
                .map(CompletableFuture::join)
                .toList();
        } catch (Exception e) {
            throw new RuntimeException("Error during parallel processing", e);
        }
    }

}
