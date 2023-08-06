package tester;

import cache.CacheInterface;
import implementations.ConcurrentLRUCache;
import implementations.ConcurrentRequestCollapsingLRU;
import implementations.RequestCollapsingLRU;
import implementations.LRUCache;
import tester.models.RType;
import tester.models.Request;
import tester.order.RandomOrganizer;
import tester.order.RequestOrganiser;
import tester.order.RotatingOrganizer;
import tester.order.SerialOrganizer;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CacheTester {
    public static void main(String args[]) {
        final int keySpace = 30, requestsPerKey = 75;
        final Map<String, Integer> inverseKey = new HashMap<>();
        var requestMap = setupRequests(keySpace, requestsPerKey, inverseKey);
        List<RequestOrganiser> organizers = new ArrayList<>();
        organizers.add(new RandomOrganizer());
        organizers.add(new SerialOrganizer());
        organizers.add(new RotatingOrganizer());
        List<CacheInterface> cacheInterfaces = new ArrayList<>();
        cacheInterfaces.add(new LRUCache(5));
        cacheInterfaces.add(new RequestCollapsingLRU(5));
        cacheInterfaces.add(new ConcurrentLRUCache(5));
        cacheInterfaces.add(new ConcurrentRequestCollapsingLRU(5));
        for (CacheInterface cache : cacheInterfaces) {
            for (RequestOrganiser organizer : organizers) {
                var requests = organizer.setOrder(keySpace, requestsPerKey, requestMap);
                System.out.println(cache.getClass().getCanonicalName() + " " + organizer.getClass().getCanonicalName());
                testCache(cache, keySpace, requestsPerKey, requestMap, inverseKey, requests);
            }
        }
        System.exit(0);
    }

    private static void testCache(CacheInterface cache, int keySpace, int requestsPerKey, List[] requestMap, Map<String, Integer> inverseKey, List<Request> requests) {
        long startTime = System.nanoTime() / 1000000000;
        //KEY, List<REQUEST>
        final var responseMap = new List[keySpace];
        final List<CompletableFuture<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < keySpace; i++) {
            responseMap[i] = new ArrayList<>();
        }
        final ExecutorService[] executorService = new ExecutorService[keySpace];
        for (int i = 0; i < executorService.length; i++) {
            executorService[i] = Executors.newSingleThreadExecutor();
        }
        for (int i = 0; i < keySpace * requestsPerKey; i++) {
            final Request request = requests.get(i);
            String key = request.getKey();
            tasks.add(CompletableFuture.runAsync(() -> {
                if (request.getType().equals(RType.GET)) {
                    responseMap[inverseKey.get(key)].add(cache.get(key));
                } else {
                    cache.put(key, request.getValue());
                }
            }, executorService[inverseKey.get(key)]));
        }
        try {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()])).get();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Problem trying to complete tasks");
            System.exit(0);
        }
        for (int i = 0; i < keySpace; i++) {
            int size = requestMap[i].size();
            String currentValue = null;
            int next = 0;
            for (int j = 0; j < size; j++) {
                Request request = (Request) requestMap[i].get(j);
                if (request.getType().equals(RType.GET)) {
                    String result = null;
                    try {
                        result = ((CompletableFuture<String>) responseMap[inverseKey.get(request.getKey())].get(next))
                                .get(2, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        System.err.println("Failed to " + request.getType() + " key: " + request.getKey() + " time: " + System.nanoTime() / 1000000000);
                        System.err.println("CURRENT REQUEST: " + j);
                        System.err.println(requestMap[i]);
                        System.err.println("CURRENT RESPONSE: " + next);
                        e.printStackTrace();
                        System.exit(0);
                    }
                    if (!currentValue.equals(result)) {
                        System.err.println("CURRENT REQUEST: " + j);
                        System.err.println(requestMap[i]);
                        System.err.println("CURRENT RESPONSE: " + next);
                        System.err.println(responseMap[inverseKey.get(request.getKey())].stream().map(c -> {
                            try {
                                return ((Future) c).get();
                            } catch (Exception e) {
                                throw new IllegalStateException();
                            }
                        }).collect(Collectors.toList()));
                        System.err.println("Mismatch in response state "
                                + result + " and expected value:" + currentValue + " for key: " + request.getKey());
                        System.exit(0);
                    } else {
                        next++;
                    }
                } else {
                    currentValue = request.getValue();
                }
            }
        }
        System.out.println("PASSED IN " + (System.nanoTime() / 1000000000d - startTime) + " SECONDS");
        System.out.println(cache.getStats());
    }

    private static List[] setupRequests(int keySpace, int requestsPerKey, Map<String, Integer> inverseKey) {
        List[] requestMap = new List[keySpace];
        for (int i = 0; i < keySpace; i++) {
            requestMap[i] = new ArrayList<Request>();
            var uuid = UUID.randomUUID().toString();
            inverseKey.put(uuid, i);
            for (int j = 0; j < requestsPerKey; j++) {
                requestMap[i].add(generateRequest(uuid, requestMap[i].size()));
            }
        }
        return requestMap;
    }

    private static Request generateRequest(String key, int requestNumber) {
        if (requestNumber == 0 || Math.random() < 0.1) {
            return new Request(RType.PUT, key, UUID.randomUUID().toString());
        } else {
            return new Request(RType.GET, key);
        }
    }
}