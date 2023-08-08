package cache.implementations;

import cache.Cache;
import cache.CacheException;
import database.DBFailure;
import database.DatabaseInterface;
import models.DoublyLinkedList;
import models.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCache extends Cache {
    private final String name;
    private final int size;
    private final DoublyLinkedList doublyLinkedList = new DoublyLinkedList();
    private final Map<String, Node> store = new HashMap<>();
    private final Lock lock = new ReentrantLock();
    private final ExecutorService[] dbQueryExecutors;
    private final LongAdder[] beingModified;
    private final boolean requestCollapsing;
    private final Statistics statistics;

    public LRUCache(String name,
                    int size,
                    int dbThreadPool,
                    boolean requestCollapsing,
                    DatabaseInterface database) {
        super(database);
        this.name = name;
        this.size = size;
        this.dbQueryExecutors = new ExecutorService[dbThreadPool];
        this.beingModified = new LongAdder[dbThreadPool];
        this.requestCollapsing = requestCollapsing;
        for (int i = 0; i < dbThreadPool; i++) {
            dbQueryExecutors[i] = Executors.newSingleThreadExecutor();
            beingModified[i] = new LongAdder();
        }
        statistics = new Statistics();
    }

    @Override
    public Future<String> get(String key) {
        if (requestCollapsing && beingModified[getHashIndex(key)].sum() == 0) {
            final Node node = store.get(key);
            if (node != null) {
                statistics.hits.increment();
                if (!node.value.isDone()) {
                    statistics.collapses.increment();
                }
                return node.value;
            } else {
                statistics.misses.increment();
            }
        } else {
            statistics.waitInQueue.increment();
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                lock.lock();
                if (store.containsKey(key)) {
                    statistics.hitsAfterWait.increment();
                    return moveToHead(key);
                }
                statistics.missesAfterWait.increment();
                evict();
                return database.get(key);
            } finally {
                lock.unlock();
            }
        }, getExecutor(key)).thenApply(future -> {
            try {
                String s = future.get(1, TimeUnit.SECONDS);
                lock.lock();
                add(key, CompletableFuture.completedFuture(s));
                lock.unlock();
                return s;
            } catch (Exception e) {
                throw wrapAndHandleException(key, e);
            }
        });
    }

    @Override
    public Future<Void> put(String key, String value) {
        beingModified[getHashIndex(key)].increment();
        return CompletableFuture.supplyAsync(() -> {
            lock.lock();
            remove(key);
            lock.unlock();
            try {
                return database.set(key, value).get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw wrapAndHandleException(key, e);
            }
        }, getExecutor(key)).thenAccept(__ -> beingModified[getHashIndex(key)].decrement());
    }

    private RuntimeException wrapAndHandleException(String key, Throwable e) {
        if (e.getCause() instanceof DBFailure) {
            lock.lock();
            remove(key);
            lock.unlock();
            return new CacheException();
        } else {
            System.err.println("Failed to get key: " + key);
            e.printStackTrace();
            return new IllegalStateException(e);
        }
    }

    private Future<String> moveToHead(String key) {
        final Node node = store.get(key);
        doublyLinkedList.delete(node);
        doublyLinkedList.updateHead(node);
        return node.value;
    }

    private void evict() {
        while (store.size() == size) {
            final Node evicted = doublyLinkedList.evict();
            store.remove(evicted.key);
            statistics.evictions.increment();
        }
    }

    private void add(String key, Future<String> result) {
        final Node node = new Node(key, result);
        doublyLinkedList.updateHead(node);
        store.put(key, node);
    }

    private void remove(String key) {
        final Node node = store.remove(key);
        if (node != null) {
            doublyLinkedList.delete(node);
        }
    }

    private ExecutorService getExecutor(String key) {
        return dbQueryExecutors[getHashIndex(key)];
    }

    private int getHashIndex(String key) {
        return Math.abs(key.hashCode()) % dbQueryExecutors.length;
    }

    public String getName() {
        return name;
    }

    public String getStats() {
        return statistics.toString() + "\n" + database.getStats();
    }
}

class Statistics {
    public LongAdder hits = new LongAdder(),
            hitsAfterWait = new LongAdder(),
            misses = new LongAdder(),
            missesAfterWait = new LongAdder(),
            evictions = new LongAdder(),
            collapses = new LongAdder(),
            waitInQueue = new LongAdder();

    @Override
    public String toString() {
        return "Statistics{" +
                "hits=" + hits.sum() +
                ", misses=" + misses.sum() +
                ", collapses=" + collapses.sum() +
                ", waitInQueue=" + waitInQueue.sum() +
                ", hitsAfterWait=" + hitsAfterWait.sum() +
                ", missesAfterWait=" + missesAfterWait.sum() +
                ", evictions=" + evictions.sum() +
                '}';
    }
}