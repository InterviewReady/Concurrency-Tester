package database;

import java.util.concurrent.CompletableFuture;

class DBCall {
    final DatabaseRequest request;
    final CompletableFuture<String> response;
    final Long startTime;

    public DBCall(DatabaseRequest request, CompletableFuture<String> response, Long startTime) {
        this.request = request;
        this.response = response;
        this.startTime = startTime;
    }
}
