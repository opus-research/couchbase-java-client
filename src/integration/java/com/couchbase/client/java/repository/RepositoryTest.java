package com.couchbase.client.java.repository;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.util.ClusterDependentTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RepositoryTest extends ClusterDependentTest {

    @Test
    public void shouldUpsertEntity() {
        User user = new User("Michael");
        repository().upsert(user);

        JsonDocument storedRaw = bucket().get(user.id());
        assertEquals("Michael", storedRaw.content().getString("name"));
    }

}
