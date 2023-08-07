package implementations;

import cache.Cache;
import models.DoublyLinkedList;
import models.Node;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentRequestCollapsingLRU extends Cache {

    final int size;
    private final DoublyLinkedList<Future<String>> doublyLinkedList = new DoublyLinkedList<>();
    Map<String, Node<Future<String>>> store = new ConcurrentHashMap<>();
    ExecutorService[] executors = new ExecutorService[10];
    ReadWriteLock lock = new ReentrantReadWriteLock();
    LongAdder[] beingModified = new LongAdder[10];

    public ConcurrentRequestCollapsingLRU(int size) {
        this.size = size;
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
//            System.out.println("1-LOCK: " + key);
                if (store.containsKey(key)) {
                    Node<Future<String>> node = store.get(key);
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
            lock.writeLock().lock();
//            System.out.println("2-LOCK: " + key);
            while (store.size() >= size) {
                Node<Future<String>> evicted = doublyLinkedList.evict();
                store.remove(evicted.key);
                evictions++;
            }
//            System.out.println("2-UNLOCK: " + key);
            lock.writeLock().unlock();
            Node<Future<String>> node = new Node<>(key, database.get(key));
            lock.writeLock().lock();
//                System.out.println("3-LOCK: " + key);
            doublyLinkedList.updateHead(node);
            store.put(key, node);
//                System.out.println("3-UNLOCK: " + key);
            lock.writeLock().unlock();
            return node.value;
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
//            System.out.println("4-LOCK: " + key);
            Node<Future<String>> node = store.remove(key);
            if (node != null) {
                doublyLinkedList.delete(node);
            }
//            System.out.println("4-UNLOCK: " + key);
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