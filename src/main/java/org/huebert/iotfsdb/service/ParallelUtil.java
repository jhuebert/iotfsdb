package org.huebert.iotfsdb.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

public class ParallelUtil {

    public static <T> void forEach(Iterable<T> iterable, Consumer<T> consumer) {
        map(iterable, item -> {
            consumer.accept(item);
            return null;
        });
    }

    public static <I, O> List<O> map(Iterable<I> iterable, Function<I, O> mapper) {
        try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {

            List<Future<O>> futures = new ArrayList<>();
            for (I item : iterable) {
                futures.add(es.submit(() -> mapper.apply(item)));
            }

            try {
                List<O> results = new ArrayList<>(futures.size());
                for (Future<O> future : futures) {
                    results.add(future.get());
                }
                return results;
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }

}
