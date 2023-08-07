package cache.implementations;

import java.util.concurrent.*;

public class ConcurrentLRUCache extends LRUCache<String> {
    ExecutorService[] executors = new ExecutorService[10];

    public ConcurrentLRUCache(int size) {
        super(size);
        for (int i = 0; i < executors.length; i++) {
            executors[i] = Executors.newSingleThreadExecutor();
        }
    }

    @Override
    public Future<String> get(String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                lock.writeLock().lock();
                if (store.containsKey(key)) {
                    hits++;
                    return moveToHead(key);
                }
            } finally {
                lock.writeLock().unlock();
            }
            misses++;
            lock.writeLock().lock();
            evictExcessKeys();
            lock.writeLock().unlock();
            try {
                String value = database.get(key).get(1, TimeUnit.SECONDS);
                lock.writeLock().lock();
                addToCache(key, value);
                lock.writeLock().unlock();
                return value;
            } catch (Exception e) {
                System.err.println("Failed to get key: " + key);
                e.printStackTrace();
                throw new IllegalStateException();
            }
        }, getKeyedExecutor(key));
    }

    @Override
    public Future<Void> put(String key, String value) {
        return CompletableFuture.supplyAsync(() -> {
            lock.writeLock().lock();
            removeFromCache(key);
            lock.writeLock().unlock();
            try {
                return database.set(key, value).get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println("Failed to get key: " + key);
                e.printStackTrace();
                throw new IllegalStateException();
            }
        }, getKeyedExecutor(key));
    }

    private ExecutorService getKeyedExecutor(String key) {
        return executors[Math.abs(key.hashCode()) % executors.length];
    }
}