package cache;

public class CacheException extends RuntimeException {
    public CacheException() {
        super("Temporary error, please retry.");
    }
}
