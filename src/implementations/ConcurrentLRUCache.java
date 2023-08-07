package implementations;

import cache.Cache;
import models.DoublyLinkedList;
import models.Node;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentLRUCache extends Cache {

    final int size;
    private final DoublyLinkedList<String> doublyLinkedList = new DoublyLinkedList<>();
    Map<String, Node<String>> store = new ConcurrentHashMap<>();
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
                    Node<String> node = store.get(key);
                    doublyLinkedList.delete(node);
                    doublyLinkedList.updateHead(node);
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
                final Node<String> evicted = doublyLinkedList.evict();
                store.remove(evicted.key);
                evictions++;
            }
//            System.out.println("2-UNLOCK: " + key);
            lock.writeLock().unlock();
            try {
                String value = database.get(key).get(1, TimeUnit.SECONDS);
                Node<String> node = new Node<>(key, value);
                lock.writeLock().lock();
//                System.out.println("3-LOCK: " + key);
                doublyLinkedList.updateHead(node);
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
            Node<String> node = store.remove(key);
            if (node != null) {
                doublyLinkedList.delete(node);
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
}