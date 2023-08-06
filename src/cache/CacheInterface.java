package cache;

import java.util.concurrent.Future;

public interface CacheInterface {
    Future<String> get(String key);
    Future<Void> put(String key, String value);
    String getStats();
}
