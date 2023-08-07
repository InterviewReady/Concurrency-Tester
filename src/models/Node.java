package models;

public class Node<T> {
    public final String key;
    public final T value;
    public Node<T> next;
    public Node<T> prev;

    public Node(String key, T value) {
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
