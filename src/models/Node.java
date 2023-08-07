package models;

import java.util.concurrent.Future;

public class Node {
    public final String key;
    public final Future<String> value;
    public Node next;
    public Node prev;

    public Node(String key, Future<String> value) {
        this.key = key;
        this.value = value;
    }

    public String toString() {
        Node current = this;
        StringBuilder s = new StringBuilder();
        int count = 0;
        while (current != null) {
            s.append(current.key).append(", ");
            current = current.next;
            count++;
            if (count > 100) {
                throw new IllegalStateException("Infinite Linked list? " + s);
            }
        }
        return s.toString();
    }
}
