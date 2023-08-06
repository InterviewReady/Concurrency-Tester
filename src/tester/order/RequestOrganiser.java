package tester.order;


import tester.models.Request;

import java.util.List;

public interface RequestOrganiser {
    List<Request> setOrder(int keySpace, int requestsPerKey, List[] requestMap);
}
