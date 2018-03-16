package com.couchbase.client.java.view;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class SpatialView implements View {

    private final String name;
    private final String map;

    protected SpatialView(String name, String map) {
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
    public String toString() {
        final StringBuilder sb = new StringBuilder("SpatialView{");
        sb.append("name='").append(name).append('\'');
        sb.append(", map='").append(map).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SpatialView that = (SpatialView) o;

        if (map != null ? !map.equals(that.map) : that.map != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (map != null ? map.hashCode() : 0);
        return result;
    }
}
