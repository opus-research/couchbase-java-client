package com.couchbase.client.java.view;

import com.couchbase.client.java.document.json.JsonObject;

/**
 * Created by michaelnitschinger on 22/07/14.
 */
public class SpatialView implements View {

    private final String name;
    private final String map;

    protected SpatialView(String name, String map) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("View name is not allowed to be null or empty.");
        }
        if (map == null || map.isEmpty()) {
            throw new IllegalArgumentException("View map function is not allowed to be null or empty.");
        }

        this.name = name;
        this.map = map;
    }

    public static View create(String name, String map) {
        return new SpatialView(name, map);
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
        return null;
    }

    @Override
    public boolean hasReduce() {
        return false;
    }

    @Override
    public boolean spatial() {
        return true;
    }

    @Override
    public String toString() {
        return "SpatialView{" +
            "name='" + name + '\'' +
            ", map='" + map + '\'' +
            '}';
    }

    @Override
    public String toJson() {
        return null;
    }
}
