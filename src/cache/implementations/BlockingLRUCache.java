package cache.implementations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BlockingLRUCache extends LRUCache<String> {

    public BlockingLRUCache(int size) {
        super(size);
    }

    @Override
    public Future<String> get(String key) {
        try {
            if (store.containsKey(key)) {
                hits++;
                return CompletableFuture.completedFuture(moveToHead(key));
            } else {
                misses++;
                evictExcessKeys();
                String result;
                try {
                    result = database.get(key).get(1, TimeUnit.SECONDS);
//                System.out.println("Fetched key: " + key + " time: " + System.nanoTime() / 1000000000);
                } catch (Exception e) {
                    System.err.println("Failed to fetch key: " + key + " time: " + System.nanoTime() / 1000000000);
                    e.printStackTrace();
                    throw new IllegalStateException();
                }
                addToCache(key, result);
                return CompletableFuture.completedFuture(result);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Future<Void> put(String key, String value) {
        try {
            lock.writeLock().lock();
            removeFromCache(key);
            try {
                database.set(key, value).get(1, TimeUnit.SECONDS);
                return CompletableFuture.completedFuture(null);
            } catch (Exception e) {
                System.err.println("Failed to set key: " + key + " time: " + System.nanoTime() / 1000000000);
                e.printStackTrace();
                throw new IllegalStateException();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}