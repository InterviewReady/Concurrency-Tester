package cache.implementations;

import cache.Cache;
import models.DoublyLinkedList;
import models.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class LRUCache<T> extends Cache {
    private final int size;
    private final DoublyLinkedList<T> doublyLinkedList = new DoublyLinkedList<>();
    protected final Map<String, Node<T>> store = new HashMap<>();
    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LRUCache(int size) {
        this.size = size;
    }

    public abstract Future<String> get(String key);
    public abstract Future<Void> put(String key, String value);

    protected T moveToHead(String key) {
        Node<T> node = store.get(key);
        doublyLinkedList.delete(node);
        doublyLinkedList.updateHead(node);
        return node.value;
    }

    protected void evictExcessKeys(){
        while (store.size() == size) {
            Node<T> evicted = doublyLinkedList.evict();
            store.remove(evicted.key);
            evictions++;
        }
    }

    protected void addToCache(String key, T result) {
        Node<T> node = new Node<>(key, result);
        doublyLinkedList.updateHead(node);
        store.put(key, node);
    }

    protected void removeFromCache(String key) {
        Node<T> node = store.remove(key);
        if (node != null) {
            doublyLinkedList.delete(node);
        }
    }
}