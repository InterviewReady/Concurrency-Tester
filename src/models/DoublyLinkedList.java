package models;

public class DoublyLinkedList {
    Node head, tail;
    public void updateHead(Node node) {
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

    public Node evict() {
        final Node deleted = tail;
        tail = tail.prev;
        if (tail == null) {
            System.err.println("HEAD when tail is null: " + head);
            throw new IllegalStateException();
        }
        tail.next = null;
        return deleted;
    }

    public void delete(Node node) {
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
