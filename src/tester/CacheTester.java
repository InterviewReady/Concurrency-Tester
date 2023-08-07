package tester;

import cache.CacheInterface;
import implementations.ConcurrentLRUCache;
import implementations.ConcurrentRequestCollapsingLRU;
import implementations.LRUCache;
import implementations.RequestCollapsingLRU;
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
        final List<RequestOrganiser> organizers = Arrays.asList(new RandomOrganizer(), new SerialOrganizer(), new RotatingOrganizer());
        final List<CacheInterface> cacheInterfaces = Arrays.asList(new LRUCache(cacheSize), new RequestCollapsingLRU(cacheSize), new ConcurrentLRUCache(cacheSize),
                new ConcurrentRequestCollapsingLRU(cacheSize));
        final int keySpace = 20, requestsPerKey = 50;
        final var requestMap = setupRequests(keySpace, requestsPerKey);
        for (final RequestOrganiser organizer : organizers) {
            final var requests = organizer.setOrder(keySpace, requestsPerKey, requestMap);
            for (final CacheInterface cache : cacheInterfaces) {
                System.out.println("Configuration: " +cache.getClass().getSimpleName() + " + " + organizer.getClass().getSimpleName());
                testCache(cache, requests);
            }
        }
        System.exit(0);
    }

    private static void testCache(CacheInterface cache, List<Request> requests) {
        final long startTime = System.nanoTime() / 1000000000;
        final List<CompletableFuture<Void>> tasks = new ArrayList<>();
        final ExecutorService[] executorService = new ExecutorService[31];
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
                    result = request.getResponse().get(2, TimeUnit.SECONDS);
                } catch (Exception e) {
                    printTrace(requests, request);
                    System.err.println("Failed to " + request.getType() + " key: " + request.getKey() + " time: " + System.nanoTime() / 1000000000);
                    e.printStackTrace();
                    System.exit(0);
                }
                if (!currentValue.get(request.getKey()).equals(result)) {
                    printTrace(requests, request);
                    System.err.println("Mismatch in response state " + result + " and expected value:" + currentValue.get(request.getKey()) + " for key: " + request.getKey());
                    System.exit(0);
                }
            } else {
                currentValue.put(request.getKey(), request.getValue());
            }
        }
        System.out.println("PASSED IN " + (System.nanoTime() / 1000000000d - startTime) + " SECONDS");
        System.out.println(cache.getStats());
    }

    private static List<Request>[] setupRequests(int keySpace, int requestsPerKey) {
        List<Request>[] requestMap = new List[keySpace];
        for (int i = 0; i < requestMap.length; i++) {
            requestMap[i] = new ArrayList<>();
            final String key = UUID.randomUUID().toString();
            requestMap[i].add(new Request(RType.PUT, key, UUID.randomUUID().toString()));
            for (int j = 1; j < requestsPerKey; j++) {
                requestMap[i].add(generateRequest(key));
            }
        }
        return requestMap;
    }

    private static Request generateRequest(String key) {
        if (Math.random() < 0.1) {
            return new Request(RType.PUT, key, UUID.randomUUID().toString());
        } else {
            return new Request(RType.GET, key);
        }
    }

    private static void printTrace(List<Request> requests, Request request) {
        System.err.println(requests.stream().filter(r -> r.getKey().equals(request.getKey())).map(r -> {
            try {
                return r.getResponse().get();
            } catch (Exception e) {
                throw new IllegalStateException();
            }
        }).collect(Collectors.toList()));
    }
}