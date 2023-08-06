package tester.order;

import tester.models.Request;

import java.util.ArrayList;
import java.util.List;

public class SerialOrganizer implements RequestOrganiser {
    @Override
    public List<Request> setOrder(int keySpace, int requestsPerKey, List[] requestMap) {
        List<Request> requests = new ArrayList<>();
        for (int i = 0; i < keySpace * requestsPerKey; i++) {
            final int index = i / requestsPerKey;
            final Request request = (Request) requestMap[index].get(i % requestsPerKey);
//            System.out.println(i + " time: " + System.nanoTime() / 1000000000 + " request: " + request);
            requests.add(request);
        }
        return requests;
    }
}
