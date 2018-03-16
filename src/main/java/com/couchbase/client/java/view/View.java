package com.couchbase.client.java.view;

public interface View {

    String name();

    String map();

    String reduce();

    boolean hasReduce();

    boolean spatial();

    String toJson();

}
