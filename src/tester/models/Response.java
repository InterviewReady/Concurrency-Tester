package tester.models;

import java.util.concurrent.Future;

public class Response {
    Future<String> result;

    public Future<String> getResult() {
        return result;
    }

    public void setResult(Future<String> future) {
        result = future;
    }
}
