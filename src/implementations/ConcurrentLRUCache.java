package implementations;

import cache.Cache;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentLRUCache extends Cache {

    final int size;
    Node head, tail;
    Map<String, Node> store = new ConcurrentHashMap<>();
    ExecutorService[] executors = new ExecutorService[10];
    ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConcurrentLRUCache(int size) {
        this.size = size;
        for (int i = 0; i < executors.length; i++) {
            executors[i] = Executors.newSingleThreadExecutor();
        }
    }

    @Override
    public Future<String> get(String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                lock.writeLock().lock();
//            System.out.println("1-LOCK: " + key);
                if (store.containsKey(key)) {
                    hits++;
                    Node node = store.get(key);
                    delete(node);
                    updateHead(node);
//                System.out.println("1-UNLOCK: " + key);
                    return node.value;
                }
            }
//            System.out.println("1-UNLOCK: " + key);
            finally {
                lock.writeLock().unlock();
            }
            misses++;
            lock.writeLock().lock();
//            System.out.println("2-LOCK: " + key);
            while (store.size() >= size) {
                evict();
                evictions++;
            }
//            System.out.println("2-UNLOCK: " + key);
            lock.writeLock().unlock();
            try {
                String value = database.get(key).get(1, TimeUnit.SECONDS);
                Node node = new Node(key, value);
                lock.writeLock().lock();
//                System.out.println("3-LOCK: " + key);
                updateHead(node);
                store.put(key, node);
//                System.out.println("3-UNLOCK: " + key);
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
//            System.out.println("4-LOCK: " + key);
            Node node = store.remove(key);
            if (node != null) {
                delete(node);
            }
//            System.out.println("4-UNLOCK: " + key);
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