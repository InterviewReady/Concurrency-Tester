package cache;

import database.Database;
import database.DatabaseInterface;

import java.util.concurrent.Future;

public abstract class Cache implements CacheInterface {
    protected final DatabaseInterface database;

    protected Cache(final DatabaseInterface database) {
        this.database = database;
    }

    public Cache() {
        this.database = new Database(5, 0.01);
    }

    @Override
    public abstract Future<String> get(String key);

    @Override
    public abstract Future<Void> put(String key, String value);
}
