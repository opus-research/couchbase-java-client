package com.couchbase.client.java.repository;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.util.ClusterDependentTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RepositoryTest extends ClusterDependentTest {

    @Test
    public void shouldUpsertEntity() {
        User user = new User("Michael", true, 1234, 55.6766);
        repository().upsert(user);

        JsonDocument storedRaw = bucket().get(user.id());
        assertEquals(user.name(), storedRaw.content().getString("name"));
        assertEquals(user.published(), storedRaw.content().getBoolean("published"));
        assertEquals(user.someNumber(), storedRaw.content().getInt("someNumber"), 0);
        assertEquals(user.otherNumber(), storedRaw.content().getDouble("otherNumber"), 0);
    }

}
