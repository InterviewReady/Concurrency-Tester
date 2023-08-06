package tester.models;

public class Request {
    final RType type;
    final String key;
    String value;
    Response response;

    public Request(RType type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public Request(RType type, String key) {
        this.type = type;
        this.key = key;
    }

    @Override
    public String toString() {
        return "{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
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

    public Response getResponse() {
        return response;
    }
}
