package database;

public class DBFailure extends RuntimeException {
    public DBFailure() {
        super("Mock failure, please retry.");
    }
}
