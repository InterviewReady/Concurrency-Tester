package cache.implementations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class RequestCollapsingLRU extends LRUCache<Future<String>> {
    private final ExecutorService executors = Executors.newFixedThreadPool(10);
    private final List<CompletableFuture<String>> reads = new ArrayList<>();

    public RequestCollapsingLRU(int size) {
        super(size);
    }

    @Override
    public Future<String> get(String key) {
        try {
            lock.writeLock().lock();
            if (store.containsKey(key)) {
                Future<String> value = moveToHead(key);
                if (value.isDone()) {
                    hits++;
                } else {
                    collapses++;
                }
                return value;
            }
        } finally {
            lock.writeLock().unlock();
        }
        misses++;
        lock.writeLock().lock();
        evictExcessKeys();
        lock.writeLock().unlock();
        try {
            lock.writeLock().lock();
            Future<String> f = database.get(key);
            final CompletableFuture<String> future =
                    CompletableFuture.supplyAsync(() -> f, executors)
                            .thenApply(value -> {
                                try {
                                    return value.get(1, TimeUnit.SECONDS);
                                } catch (Exception e) {
                                    System.err.println("Failed to get key: " + key);
                                    e.printStackTrace();
                                    throw new IllegalStateException();
                                }
                            });
            addToCache(key, f);
            reads.add(future);
            return f;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Future<Void> put(String key, String value) {
        try {
            lock.writeLock().lock();
            CompletableFuture.allOf(reads.toArray(new CompletableFuture[reads.size()])).get();
            removeFromCache(key);
            database.set(key, value).get(1, TimeUnit.SECONDS);
            lock.writeLock().unlock();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            System.err.println("Failed to get key: " + key);
            e.printStackTrace();
            throw new IllegalStateException();
        }
    }
}