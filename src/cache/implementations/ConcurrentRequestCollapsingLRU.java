package cache.implementations;

import models.Node;

import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class ConcurrentRequestCollapsingLRU extends LRUCache<Future<String>> {

    ExecutorService[] executors = new ExecutorService[10];
    LongAdder[] beingModified = new LongAdder[10];

    public ConcurrentRequestCollapsingLRU(int size) {
        super(size);
        for (int i = 0; i < executors.length; i++) {
            executors[i] = Executors.newSingleThreadExecutor();
        }
        for (int i = 0; i < beingModified.length; i++) {
            beingModified[i] = new LongAdder();
        }
    }

    @Override
    public Future<String> get(String key) {
        boolean notModified = beingModified[getHashIndex(key)].sum() == 0;
        if (notModified) {
            Node<Future<String>> futureNode = store.get(key);
            if (futureNode != null) {
                hits++;
                return futureNode.value;
            } else {
                misses++;
            }
        } else {
            waitForWrite++;
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                lock.writeLock().lock();
                if (store.containsKey(key)) {
                    return moveToHead(key);
                }
            } finally {
                lock.writeLock().unlock();
            }
            lock.writeLock().lock();
            evictExcessKeys();
            lock.writeLock().unlock();
            lock.writeLock().lock();
            Future<String> value = database.get(key);
            addToCache(key, value);
            lock.writeLock().unlock();
            return value;
        }, getKeyedExecutor(key)).thenApply(future -> {
            try {
                return future.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println("Failed to get key: " + key);
                e.printStackTrace();
                throw new IllegalStateException();
            }
        });
    }

    @Override
    public Future<Void> put(String key, String value) {
        beingModified[getHashIndex(key)].increment();
        return CompletableFuture.supplyAsync(() -> {
            lock.writeLock().lock();
            removeFromCache(key);
            lock.writeLock().unlock();
            try {
                Void unused = database.set(key, value).get(1, TimeUnit.SECONDS);
                beingModified[getHashIndex(key)].decrement();
                return unused;
            } catch (Exception e) {
                System.err.println("Failed to get key: " + key);
                e.printStackTrace();
                throw new IllegalStateException();
            }
        }, getKeyedExecutor(key));
    }

    private ExecutorService getKeyedExecutor(String key) {
        return executors[getHashIndex(key)];
    }

    private int getHashIndex(String key) {
        return Math.abs(key.hashCode()) % executors.length;
    }

}