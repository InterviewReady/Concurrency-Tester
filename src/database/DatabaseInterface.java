package database;

import java.util.concurrent.Future;

public interface DatabaseInterface {
    Future<String> get(String key);
    Future<Void> set(String key, String value);
    String getStats();
}
