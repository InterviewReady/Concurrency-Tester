package tester.order;

import tester.models.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomOrganizer implements RequestOrganiser {
    public List<Request> setOrder(int keySpace, int requestsPerKey, List[] requestMap) {
        List<Request> requests = new ArrayList<>();
        final int[] currentPointer = new int[keySpace];
        int currentSpace = keySpace;
        final var random = new Random();
        for (int i = 0; i < keySpace * requestsPerKey; i++) {
            final int index = random.nextInt(currentSpace);
            final Request request = (Request) requestMap[index].get(currentPointer[index]);
//            System.out.println(i + " time: " + System.nanoTime() / 1000000000 + " request: " + request);
            requests.add(request);
            currentPointer[index]++;
            if (currentPointer[index] == requestMap[index].size()) {
                currentSpace--;
                currentPointer[index] = currentPointer[currentSpace];
                var temp = requestMap[index];
                requestMap[index] = requestMap[currentSpace];
                requestMap[currentSpace] = temp;
            }
        }
        return requests;
    }

}
