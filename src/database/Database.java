package database;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a concurrent database with tunable failure rate and no ordering guarantees for responses.
 * This database handles GET and SET operations, processes requests concurrently, and tracks statistics.
 */
public class Database implements DatabaseInterface {

    // Configuration and state variables
    private final int batchRequestThreshold;
    private final Map<String, String> db;
    private final Map<String, LongAdder> requestCount;
    private final List<DBCall> pendingCalls;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ScheduledExecutorService executorService;
    private final double failureRate;

    // Metrics counters
    private final LongAdder batchCompletion = new LongAdder(),
            clearance = new LongAdder(),
            concurrentRequests = new LongAdder(),
            failures = new LongAdder(),
            hits = new LongAdder();

    /**
     * Constructs a Database instance with specified batch processing threshold and failure rate.
     *
     * @param batchThreshold The threshold for batch processing of requests.
     * @param failureRate    The tunable failure rate for simulated failures.
     */
    public Database(final int batchThreshold, double failureRate) {
        this.batchRequestThreshold = batchThreshold;
        this.failureRate = failureRate;
        db = new HashMap<>();
        requestCount = new ConcurrentHashMap<>(batchRequestThreshold);
        pendingCalls = new ArrayList<>();
        executorService = Executors.newSingleThreadScheduledExecutor();
        // Schedule batch processing task to run periodically
        executorService.scheduleAtFixedRate(this::completePendingRequests, 0, 1, TimeUnit.MILLISECONDS);
    }

    /**
     * Retrieves the value associated with the specified key from the database.
     *
     * @param key The key for which to retrieve the value.
     * @return A Future representing the asynchronous result of the GET operation.
     */
    public Future<String> get(String key) {
        hits.increment(); // Increment the hits counter
        return addToRequestQueue(new DatabaseRequest(DBRType.GET, key));
    }

    /**
     * Sets the value associated with the specified key in the database.
     *
     * @param key   The key to set.
     * @param value The value to set.
     * @return A Future representing the asynchronous result of the SET operation.
     */
    public Future<Void> set(String key, String value) {
        // Add the SET request to the queue and complete it with a null value
        return addToRequestQueue(new DatabaseRequest(DBRType.SET, key, value))
                .thenAccept(__ -> {
                });
    }

    /**
     * Adds a database request to the pending queue for processing.
     *
     * @param databaseRequest The database request to enqueue.
     * @return A CompletableFuture representing the asynchronous response to the request.
     */
    private CompletableFuture<String> addToRequestQueue(DatabaseRequest databaseRequest) {
        DBCall dbCall = new DBCall(databaseRequest, new CompletableFuture<>(), System.nanoTime());
        lock.writeLock().lock();
        requestCount.putIfAbsent(databaseRequest.key, new LongAdder());
        LongAdder count = requestCount.get(databaseRequest.key);
        count.increment();
        if (count.sum() > 1) {
            concurrentRequests.increment();
        }
        pendingCalls.add(dbCall);
        lock.writeLock().unlock();
        // Trigger batch processing if the threshold is reached
        if (pendingCalls.size() >= batchRequestThreshold) {
            executorService.execute(this::completePendingRequests);
        }
        return dbCall.response;
    }

    /**
     * Processes pending database requests, allowing for concurrent processing.
     */
    private void completePendingRequests() {
        if (!pendingCalls.isEmpty()) {
            lock.writeLock().lock();
            if (!pendingCalls.isEmpty()) {
                boolean clearAll = pendingCalls.size() >= batchRequestThreshold;
                if (clearAll) {
                    batchCompletion.increment();
                }
                List<DBCall> completedRequests = new ArrayList<>();
                Collections.shuffle(pendingCalls); // Randomize order for no ordering guarantees
                for (final var call : pendingCalls) {
                    final boolean oldEntry = System.nanoTime() - call.startTime > 1000000;
                    final DatabaseRequest request = call.request;
                    final CompletableFuture<String> response = call.response;
                    if (Math.random() < failureRate) { // Simulate a failure
                        failures.increment(); // Increment failure counter
                        call.response.completeExceptionally(new DBFailure());
                        completedRequests.add(call);
                    } else if (clearAll || oldEntry) { // Process the request
                        if (!clearAll) {
                            clearance.increment(); // Increment clearance counter
                        }
                        if (request.type.equals(DBRType.GET)) {
                            response.complete(getKey(request.key)); // Complete with retrieved value
                        } else {
                            setKey(request.key, request.value); // Set value in the database
                            response.complete(null); // Complete with null since SET doesn't return a value
                        }
                        completedRequests.add(call);
                    }
                }
                // Remove completed requests from the pendingCalls list and decrement request count
                completedRequests.forEach(dbCall -> {
                    pendingCalls.remove(dbCall);
                    requestCount.get(dbCall.request.key).decrement();
                });
            }
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the value associated with the specified key from the database.
     *
     * @param key The key for which to retrieve the value.
     * @return The value associated with the key, or null if the key is not found.
     */
    private String getKey(String key) {
        return db.get(key);
    }

    /**
     * Sets the value associated with the specified key in the database.
     *
     * @param key   The key to set.
     * @param value The value to set.
     */
    private void setKey(String key, String value) {
        db.put(key, value);
    }

    /**
     * Returns statistics about the database's performance and operation.
     *
     * @return A formatted string containing various statistics.
     */
    @Override
    public String getStats() {
        // Return a formatted string containing various statistics
        return "clearances: " + clearance.sum()
                + " batchCompletions: " + batchCompletion.sum()
                + " concurrentRequests: " + concurrentRequests.sum()
                + " failures: " + failures.sum()
                + " hits: " + hits.sum();
    }
}