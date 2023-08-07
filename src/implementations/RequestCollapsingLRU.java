package implementations;

import cache.Cache;
import models.DoublyLinkedList;
import models.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RequestCollapsingLRU extends Cache {

    final int size;
    private final DoublyLinkedList<Future<String>> doublyLinkedList = new DoublyLinkedList<>();
    Map<String, Node<Future<String>>> store = new ConcurrentHashMap<>();
    ExecutorService executors = Executors.newFixedThreadPool(10);
    List<CompletableFuture<String>> reads = new ArrayList<>();
    ReadWriteLock lock = new ReentrantReadWriteLock();

    public RequestCollapsingLRU(int size) {
        this.size = size;
    }

    @Override
    public Future<String> get(String key) {
        try {
            lock.writeLock().lock();
            if (store.containsKey(key)) {
                Node<Future<String>> node = store.get(key);
                if (node.value.isDone()) {
                    hits++;
                } else {
                    collapses++;
                }
                doublyLinkedList.delete(node);
                doublyLinkedList.updateHead(node);
                return node.value;
            }
        } finally {
            lock.writeLock().unlock();
        }
        misses++;
        lock.writeLock().lock();
        while (store.size() >= size) {
            Node<Future<String>> evicted = doublyLinkedList.evict();
            store.remove(evicted.key);
            evictions++;
        }
        lock.writeLock().unlock();
        try {
            lock.writeLock().lock();
            Node<Future<String>> node = new Node<>(key, database.get(key));
            Node<Future<String>> otherNode = store.putIfAbsent(key, node);
            if (otherNode == null) {
                doublyLinkedList.updateHead(node);
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
                doublyLinkedList.updateHead(otherNode);
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
            Node<Future<String>> node = store.remove(key);
            if (node != null) {
                doublyLinkedList.delete(node);
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
}