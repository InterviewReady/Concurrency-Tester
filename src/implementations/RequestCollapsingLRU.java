package implementations;

import cache.Cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RequestCollapsingLRU extends Cache {

    final int size;
    FutureNode head, tail;
    Map<String, FutureNode> store = new ConcurrentHashMap<>();
    ExecutorService executors = Executors.newFixedThreadPool(10);
    List<CompletableFuture> reads = new ArrayList<>();
    ReadWriteLock lock = new ReentrantReadWriteLock();

    public RequestCollapsingLRU(int size) {
        this.size = size;
    }

    @Override
    public Future<String> get(String key) {
        try {
            lock.writeLock().lock();
            if (store.containsKey(key)) {
                FutureNode node = store.get(key);
                if (node.value.isDone()) {
                    hits++;
                } else {
                    collapses++;
                }
                delete(node);
                updateHead(node);
                return node.value;
            }
        } finally {
            lock.writeLock().unlock();
        }
        misses++;
        lock.writeLock().lock();
        while (store.size() >= size) {
            evict();
            evictions++;
        }
        lock.writeLock().unlock();
        try {
            lock.writeLock().lock();
            FutureNode node = new FutureNode(key, database.get(key));
            FutureNode otherNode = store.putIfAbsent(key, node);
            if (otherNode == null) {
                updateHead(node);
                final CompletableFuture<String> future =
                        CompletableFuture.supplyAsync(() -> node.value, executors)
                                .thenApply(value -> {
                                    try {
                                        return value.get(1, TimeUnit.SECONDS);
                                    } catch (Exception e) {
                                        System.err.println("Failed to get key: " + key);
                                        e.printStackTrace();
                                        throw new IllegalStateException();
                                    }
                                });
                reads.add(future);
            } else {
                System.out.println("Collapsed request");
                collapses++;
                updateHead(otherNode);
            }
            return store.get(key).value;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Future<Void> put(String key, String value) {
        try {
            lock.writeLock().lock();
            CompletableFuture.allOf(reads.toArray(new CompletableFuture[reads.size()])).get();
            FutureNode node = store.remove(key);
            if (node != null) {
                delete(node);
            }
            database.set(key, value).get(1, TimeUnit.SECONDS);
            lock.writeLock().unlock();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            System.err.println("Failed to get key: " + key);
            e.printStackTrace();
            throw new IllegalStateException();
        }
    }

    private void updateHead(FutureNode node) {
        node.next = head;
        node.prev = null;
        if (head != null) {
            head.prev = node;
        }
        head = node;
        if (tail == null) {
            tail = node;
        }
    }

    private void evict() {
        store.remove(tail.key);
        tail = tail.prev;
        if (tail == null) {
            System.err.println("TAIL is null for store: " + store.keySet());
            System.err.println("HEAD when tail is null: " + head);
            throw new IllegalStateException();
        }
        tail.next = null;
    }

    private void delete(FutureNode node) {
        if (head == node)
            head = node.next;
        if (tail == node)
            tail = node.prev;
        if (node.next != null)
            node.next.prev = node.prev;
        if (node.prev != null)
            node.prev.next = node.next;
    }
}

