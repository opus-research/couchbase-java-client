package com.couchbase.client.java.repository;


import com.couchbase.client.java.repository.mapping.annotation.Field;
import com.couchbase.client.java.repository.mapping.annotation.Id;

public class User {

    @Id
    private final String id;

    @Field
    private final String name;

    @Field
    private final boolean published;

    @Field
    private final String nullField = null;

    @Field
    private final int someNumber;

    @Field
    private final double otherNumber;

    public User(String name, boolean published, int someNumber, double otherNumber) {
        id = "user::" + name;
        this.name = name;
        this.published = published;
        this.someNumber = someNumber;
        this.otherNumber = otherNumber;
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    public boolean published() {
        return published;
    }

    public int someNumber() {
        return someNumber;
    }

    public double otherNumber() {
        return otherNumber;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("User{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", published=").append(published);
        sb.append(", someNumber=").append(someNumber);
        sb.append(", otherNumber=").append(otherNumber);
        sb.append('}');
        return sb.toString();
    }
}
