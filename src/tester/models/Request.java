package tester.models;

import java.util.UUID;
import java.util.concurrent.Future;

public class Request {
    final RType type;
    final String key;
    String value;
    Response response;
    String id;

    public Request(RType type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.id = UUID.randomUUID().toString();
        response = new Response();
    }

    public Request(RType type, String key) {
        this(type, key, null);
    }

    @Override
    public String toString() {
        return "{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", id='" + id + '\'' +
                ", response='" + response.getResult() + '\'' +
                '}';
    }

    public RType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public Future getResponse() {
        return response.getResult();
    }

    public void setResponse(Future future) {
        response.setResult(future);
    }
}
