package implementations;

import cache.Cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUCache extends Cache {

    final int size;
    Node head, tail;
    Map<String, Node> store = new HashMap<>();
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public LRUCache(int size) {
        this.size = size;
    }

    @Override
    public Future<String> get(String key) {
        try {
            readWriteLock.writeLock().lock();
            if (store.containsKey(key)) {
                hits++;
                Node node = store.get(key);
                delete(node);
                updateHead(node);
                return CompletableFuture.completedFuture(node.value);
            } else {
                misses++;
                if (store.size() == size) {
                    evictions++;
                    evict();
                }
                Future<String> future = database.get(key);
                String result;
                try {
                    result = future.get(1, TimeUnit.SECONDS);
//                System.out.println("Fetched key: " + key + " time: " + System.nanoTime() / 1000000000);
                } catch (Exception e) {
                    System.err.println("Failed to fetch key: " + key + " time: " + System.nanoTime() / 1000000000);
                    e.printStackTrace();
                    throw new IllegalStateException();
                }
                Node node = new Node(key, result);
                updateHead(node);
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
            Node node = store.remove(key);
            if (node != null) {
                delete(node);
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

    private void updateHead(Node node) {
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

    private void delete(Node node) {
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

