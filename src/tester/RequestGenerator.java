package tester;

import tester.models.RType;
import tester.models.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RequestGenerator {
    private final double writeProbability;

    public RequestGenerator(double writeProbability) {
        this.writeProbability = writeProbability;
    }

    public List<Request>[] setupRequests(int keySpace, int requestsPerKey) {
        List<Request>[] requestMap = new List[keySpace];
        for (int i = 0; i < requestMap.length; i++) {
            requestMap[i] = new ArrayList<>();
            final String key = UUID.randomUUID().toString();
            requestMap[i].add(new Request(RType.PUT, key, UUID.randomUUID().toString()));
            for (int j = 1; j < requestsPerKey; j++) {
                requestMap[i].add(generateRequest(key, writeProbability));
            }
        }
        return requestMap;
    }

    private Request generateRequest(String key, double writeProbability) {
        if (Math.random() < writeProbability) {
            return new Request(RType.PUT, key, UUID.randomUUID().toString());
        } else {
            return new Request(RType.GET, key);
        }
    }

    public double getWriteProbability() {
        return writeProbability;
    }
}
