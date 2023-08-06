package database;

class DatabaseRequest {
    final DBRType type;
    final String key;
    final String value;

    public DatabaseRequest(DBRType type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public DatabaseRequest(DBRType type, String key) {
        if (type.equals(DBRType.SET)) {
            throw new IllegalArgumentException("Cannot create a DB set request without value");
        }
        this.type = type;
        this.key = key;
        this.value = null;
    }

    @Override
    public String toString() {
        return "{" +
                "type=" + type +
                ", key='" + key + '\'' +
                (type.equals(DBRType.SET) ? ", value='" + value + '\'' : "") +
                '}';
    }
}
