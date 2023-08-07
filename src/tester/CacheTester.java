package tester;

import cache.implementations.LRUCache;
import tester.models.RType;
import tester.models.Request;
import tester.order.RandomOrganizer;
import tester.order.RequestOrganiser;
import tester.order.RotatingOrganizer;
import tester.order.SerialOrganizer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CacheTester {
    public static void main(String args[]) {
        int cacheSize = 5;
        final List<RequestOrganiser> organizers = Arrays.asList(
                new RandomOrganizer(),
                new SerialOrganizer(),
                new RotatingOrganizer());
        final List<RequestGenerator> generators = Arrays.asList(
                new RequestGenerator(0.1),
                new RequestGenerator(0.5),
                new RequestGenerator(0.01)
        );
        final int keySpace = 20, requestsPerKey = 50;
        for (final RequestGenerator generator : generators) {
            final var requestMap = generator.setupRequests(keySpace, requestsPerKey);
            for (final RequestOrganiser organizer : organizers) {
                final var requests = organizer.setOrder(keySpace, requestsPerKey, requestMap);
                final List<LRUCache> cacheInterfaces = Arrays.asList(
                        new LRUCache("Blocking Cache", cacheSize, 1, false),
                        new LRUCache("Blocking request collapsing cache", cacheSize, 1, true),
                        new LRUCache("Concurrent Cache", cacheSize, 10, false),
                        new LRUCache("Concurrent request collapsing cache", cacheSize, 10, true)
                );
                for (final LRUCache cache : cacheInterfaces) {
                    System.out.println("Configuration: " + cache.getName() + " + " + organizer.getClass().getSimpleName() + " + writeProbability: " + generator.getWriteProbability());
                    testCache(cache, requests);
                }
            }
        }
        System.exit(0);
    }

    private static void testCache(LRUCache cache, List<Request> requests) {
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
        }
        try {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()])).get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Problem when completing tasks");
            System.exit(0);
        }
        final Map<String, String> currentValue = new HashMap<>();
        for (final Request request : requests) {
            if (request.getType().equals(RType.GET)) {
                String result = null;
                try {
                    result = request.getResponse().get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    System.err.println("Failed to " + request.getType() + " key: " + request.getKey() + " time: " + System.nanoTime() / 1000000000);
                    e.printStackTrace();
                    printTraceAndExit(requests, request);
                }
                if (!currentValue.get(request.getKey()).equals(result)) {
                    System.err.println("Mismatch in response state: " + result + " and expected value:" + currentValue.get(request.getKey()) + " for key: " + request.getKey());
                    printTraceAndExit(requests, request);
                }
            } else {
                currentValue.put(request.getKey(), request.getValue());
            }
        }
        System.out.println("PASSED IN " + (System.nanoTime() / 1000000000d - startTime) + " SECONDS");
        System.out.println(cache.getStats());
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