package implementations;

import java.util.concurrent.Future;

class FutureNode {
    String key;
    Future<String> value;
    FutureNode next;
    FutureNode prev;

    public FutureNode(String key, Future<String> value) {
        this.key = key;
        this.value = value;
    }

    public String toString() {
        FutureNode current = this;
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
