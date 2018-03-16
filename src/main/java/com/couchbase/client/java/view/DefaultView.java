package com.couchbase.client.java.view;

import com.couchbase.client.java.document.json.JsonObject;

public class DefaultView implements View {

    private final String name;
    private final String map;
    private final String reduce;

    protected DefaultView(String name, String map, String reduce) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("View name is not allowed to be null or empty.");
        }
        if (map == null || map.isEmpty()) {
            throw new IllegalArgumentException("View map function is not allowed to be null or empty.");
        }

        this.name = name;
        this.map = map;
        this.reduce = reduce;
    }

    public static View create(String name, String map, String reduce) {
        return new DefaultView(name, map, reduce);
    }

    public static View create(String name, String map) {
        return new DefaultView(name, map, null);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String map() {
        return map;
    }

    @Override
    public String reduce() {
        return reduce;
    }

    @Override
    public boolean hasReduce() {
        return reduce != null && !reduce.isEmpty();
    }

    @Override
    public boolean spatial() {
        return false;
    }

    @Override
    public String toString() {
        return "DefaultView{" +
            "name='" + name + '\'' +
            ", map='" + map + '\'' +
            ", reduce='" + reduce + '\'' +
            '}';
    }

    @Override
    public String toJson() {
        return null;
    }
}
