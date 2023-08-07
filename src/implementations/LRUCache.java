package implementations;

import cache.Cache;
import models.DoublyLinkedList;
import models.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUCache extends Cache {

    private final int size;
    private final Map<String, Node<String>> store = new HashMap<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final DoublyLinkedList<String> list = new DoublyLinkedList<>();

    public LRUCache(int size) {
        this.size = size;
    }

    @Override
    public Future<String> get(String key) {
        try {
            readWriteLock.writeLock().lock();
            if (store.containsKey(key)) {
                hits++;
                Node<String> node = store.get(key);
                list.delete(node);
                list.updateHead(node);
                return CompletableFuture.completedFuture(node.value);
            } else {
                misses++;
                if (store.size() == size) {
                    Node<String> evicted = list.evict();
                    store.remove(evicted.key);
                    evictions++;
                }
                String result;
                try {
                    result = database.get(key).get(1, TimeUnit.SECONDS);
//                System.out.println("Fetched key: " + key + " time: " + System.nanoTime() / 1000000000);
                } catch (Exception e) {
                    System.err.println("Failed to fetch key: " + key + " time: " + System.nanoTime() / 1000000000);
                    e.printStackTrace();
                    throw new IllegalStateException();
                }
                Node<String> node = new Node<>(key, result);
                list.updateHead(node);
                store.put(key, node);
                return CompletableFuture.completedFuture(result);
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public Future<Void> put(String key, String value) {
        try {
            readWriteLock.writeLock().lock();
            Node<String> node = store.remove(key);
            if (node != null) {
                list.delete(node);
            }
            try {
                database.set(key, value).get(1, TimeUnit.SECONDS);
                return CompletableFuture.completedFuture(null);
            } catch (Exception e) {
                System.err.println("Failed to set key: " + key + " time: " + System.nanoTime() / 1000000000);
                e.printStackTrace();
                throw new IllegalStateException();
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}