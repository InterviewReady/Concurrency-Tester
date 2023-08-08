package tester;

import cache.Cache;
import cache.CacheException;
import cache.implementations.LRUCache;
import database.Database;
import tester.models.RType;
import tester.models.Request;
import tester.order.RequestOrganiser;
import tester.order.RotatingOrganizer;
import tester.order.SerialOrganizer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SingleCacheTester {
    public static void main(String[] args) {
        final List<RequestOrganiser> organizers = Arrays.asList(
                new SerialOrganizer(),
                new RotatingOrganizer());
        final int keySpace = 40, requestsPerKey = 100;
        final var generator = new RequestGenerator(0.01);
        final var requestMap = generator.setupRequests(keySpace, requestsPerKey);
        for (final RequestOrganiser organizer : organizers) {
            final var requests = organizer.setOrder(keySpace, requestsPerKey, requestMap);
            for (int factor = 1; factor <= 2; factor++) {
                final int cacheSize = keySpace / factor;
                Database database = new Database(5, 0.01);
                Cache cache = new LRUCache("", cacheSize, 1, false, database);
                System.out.println("Configuration: " + organizer.getClass().getSimpleName()
                        + " + cacheSize: " + (100.0 / factor));
                testCache(cache, requests);
                System.out.println(database.getStats());
            }
        }
        System.exit(0);
    }

    private static void testCache(Cache cache, List<Request> requests) {
        final long startTime = System.nanoTime() / 1000000000;
        final List<CompletableFuture<Void>> tasks = new ArrayList<>();
        final ExecutorService[] executorService = new ExecutorService[3];
        for (int i = 0; i < executorService.length; i++) {
            executorService[i] = Executors.newSingleThreadExecutor();
        }
        for (final Request request : requests) {
            final String key = request.getKey();
            tasks.add(CompletableFuture.runAsync(() -> {
                if (request.getType().equals(RType.GET)) {
                    request.setResponse(cache.get(key));
                } else {
                    request.setResponse(cache.put(key, request.getValue()));
                }
            }, executorService[Math.abs(key.hashCode()) % executorService.length]));
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                System.err.println("Thread sleep issues for request: " + request);
                throw new RuntimeException(e);
            }
        }
        try {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()])).get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Problem when completing tasks");
            System.exit(0);
        }
        int cacheFailures = 0;
        final Map<String, String> currentValue = new HashMap<>();
        for (final Request request : requests) {
            Object result = null;
            boolean cacheFailure = false;
            try {
                result = request.getResponse().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (e.getCause() instanceof CacheException) {
                    cacheFailure = true;
                    cacheFailures++;
                } else {
                    System.err.println("Failed to " + request.getType() + " key: " + request.getKey() + " time: " + System.nanoTime() / 1000000000);
                    e.printStackTrace();
                    printTraceAndExit(requests, request);
                }
            }
            if (!cacheFailure) {
                if (request.getType().equals(RType.GET)) {
                    if (!Objects.equals(currentValue.get(request.getKey()), result)) {
                        System.err.println("Mismatch in response state: " + result + " and expected value:" + currentValue.get(request.getKey()) + " for key: " + request.getKey());
                        printTraceAndExit(requests, request);
                    }
                } else {
                    currentValue.put(request.getKey(), request.getValue());
                }
            }
        }
        System.out.println("PASSED IN " + (System.nanoTime() / 1000000000d - startTime) + " SECONDS");
    }

    private static void printTraceAndExit(List<Request> requests, Request request) {
        System.err.println(requests.stream().filter(r -> r.getKey().equals(request.getKey())).map(r -> {
            try {
                return r.getResponse().get();
            } catch (Exception e) {
                throw new IllegalStateException();
            }
        }).collect(Collectors.toList()));
        System.exit(0);
    }
}