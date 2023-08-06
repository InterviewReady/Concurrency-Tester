package cache;

import database.Database;
import database.DatabaseInterface;

import java.util.concurrent.Future;

public abstract class Cache implements CacheInterface {

    protected final DatabaseInterface database = new Database(4);
    protected int hits = 0, misses = 0, evictions = 0, collapses = 0, waitForWrite = 0;

    @Override
    public abstract Future<String> get(String key);

    @Override
    public abstract Future<Void> put(String key, String value);

    @Override
    public String getStats() {
        return "Cache hits: " + hits + " misses: " + misses + " evictions: " + evictions + " collapses: " + collapses + " waitForWrites: " + waitForWrite + "\nDatabase stats: " + database.getStats();
    }
}
