package com.couchbase.client.java.repository;


import com.couchbase.client.java.repository.mapping.annotation.Field;
import com.couchbase.client.java.repository.mapping.annotation.Id;

public class User {

    @Id
    private final String id;

    @Field
    private final String name;

    public User(String name) {
        id = "user::" + name;
        this.name = name;
    }

    public String id() {
        return id;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("User{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
