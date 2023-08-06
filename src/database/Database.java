package database;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database implements DatabaseInterface {
    private final int batchRequestThreshold;
    private final Map<String, String> db;
    private final Map<String, LongAdder> requestCount;
    private final List<DBCall> pendingCalls;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ScheduledExecutorService executorService;

    private int batchCompletion, clearance, concurrentRequests, hits;

    public Database(final int batchThreshold) {
        this.batchRequestThreshold = batchThreshold;
        db = new HashMap<>();
        requestCount = new ConcurrentHashMap<>(batchRequestThreshold);
        pendingCalls = new ArrayList<>();
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(this::completePendingRequests, 0, 10, TimeUnit.MILLISECONDS);
    }

    public Future<String> get(String key) {
        hits++;
        return addToRequestQueue(new DatabaseRequest(DBRType.GET, key));
    }

    public Future<Void> set(String key, String value) {
        return addToRequestQueue(new DatabaseRequest(DBRType.SET, key, value))
                .thenAccept(__ -> {
                });
    }

    private CompletableFuture<String> addToRequestQueue(DatabaseRequest databaseRequest) {
        DBCall dbCall = new DBCall(databaseRequest, new CompletableFuture<>(), System.nanoTime());
        lock.writeLock().lock();
        requestCount.putIfAbsent(databaseRequest.key, new LongAdder());
        LongAdder count = requestCount.get(databaseRequest.key);
        count.increment();
        if (count.sum() > 1) {
            concurrentRequests++;
        }
        pendingCalls.add(dbCall);
        lock.writeLock().unlock();
        if (pendingCalls.size() >= batchRequestThreshold) {
            executorService.execute(this::completePendingRequests);
        }
        return dbCall.response;
    }

    private void completePendingRequests() {
        if (!pendingCalls.isEmpty()) {
            lock.writeLock().lock();
            if (!pendingCalls.isEmpty()) {
                boolean clearAll = pendingCalls.size() >= batchRequestThreshold;
                if (clearAll) {
                    batchCompletion++;
                }
                List<DBCall> completedRequests = new ArrayList<>();
                Collections.shuffle(pendingCalls);
                for (final var call : pendingCalls) {
                    boolean oldEntry = System.nanoTime() - call.startTime > 10000000;
                    if (clearAll || oldEntry) {
                        if (!clearAll) {
                            clearance++;
                        }
                        DatabaseRequest request = call.request;
                        CompletableFuture<String> response = call.response;
                        if (request.type.equals(DBRType.GET)) {
                            response.complete(getKey(request.key));
                        } else {
                            setKey(request.key, request.value);
                            response.complete(null);
                        }
                        completedRequests.add(call);
                    }
                }
                completedRequests.forEach(dbCall -> {
                    pendingCalls.remove(dbCall);
                    String key = dbCall.request.key;
                    requestCount.get(key).decrement();
                });
            }
            lock.writeLock().unlock();
        }
    }

    private String getKey(String key) {
        return db.get(key);
    }

    private void setKey(String key, String value) {
        db.put(key, value);
    }

    @Override
    public String getStats() {
        return "clearances: " + clearance
                + " batchCompletions: " + batchCompletion
                + " concurrentRequests: " + concurrentRequests
                + " hits: " + hits;
    }
}

