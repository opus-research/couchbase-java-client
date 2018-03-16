/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.java;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.util.ClusterDependentTest;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.*;

public class BinaryTest extends ClusterDependentTest {

    @Test
    public void shouldGetNonexistentWithDefault() {
        JsonDocument jsonDocument = bucket().get("i-dont-exist");
        assertNull(jsonDocument);
    }

    @Test(expected = DocumentAlreadyExistsException.class)
    public void shouldErrorOnDoubleInsert() {
        String id = "double-insert";
        JsonObject content = JsonObject.empty().put("hello", "world");
        final JsonDocument doc = JsonDocument.create(id, content);
        bucket().insert(doc);
        bucket().insert(doc);
    }

    @Test
    public void shouldInsertAndGet() {
        JsonObject content = JsonObject.empty().put("hello", "world");
        final JsonDocument doc = JsonDocument.create("insert", content);

        bucket().insert(doc);
        JsonDocument response = bucket().get("insert");
        assertEquals(content.getString("hello"), response.content().getString("hello"));
    }

    @Test
    public void shouldUpsertAndGetAndRemove() {
        JsonObject content = JsonObject.empty().put("hello", "world");
        final JsonDocument doc = JsonDocument.create("upsert", content);

        bucket().upsert(doc);
        JsonDocument response = bucket().get("upsert");
        assertEquals(content.getString("hello"), response.content().getString("hello"));

        JsonDocument removed = bucket().remove(doc);
        assertEquals(doc.id(), removed.id());
        assertNull(removed.content());
        assertEquals(0, removed.expiry());
        assertTrue(removed.cas() != 0);

        assertNull(bucket().get("upsert"));
    }

  @Test
  public void shouldUpsertAndReplace() {
    JsonObject content = JsonObject.empty().put("hello", "world");
    final JsonDocument doc = JsonDocument.create("upsert-r", content);
    bucket().upsert(doc);
    JsonDocument response = bucket().get("upsert-r");
    assertEquals(content.getString("hello"), response.content().getString("hello"));

    JsonDocument updated = JsonDocument.from(response, JsonObject.empty().put("hello", "replaced"));
    bucket().replace(updated);
    response = bucket().get("upsert-r");
    assertEquals("replaced", response.content().getString("hello"));
  }

    @Test
    public void shouldIncrementFromCounter() throws Exception {
        JsonLongDocument doc1 = bucket().counter("incr-key", 10, 0, 0);
        assertEquals(0L, (long) doc1.content());

        JsonLongDocument doc2 = bucket().counter("incr-key", 10, 0, 0);
        assertEquals(10L, (long) doc2.content());

        JsonLongDocument doc3 = bucket().counter("incr-key", 10, 0, 0);
        assertEquals(20L, (long) doc3.content());

        assertTrue(doc1.cas() != doc2.cas());
        assertTrue(doc1.cas() != doc3.cas());
        assertTrue(doc2.cas() != doc3.cas());
    }

    @Test
    public void shouldDecrementFromCounter() throws Exception {
        JsonLongDocument doc1 = bucket().counter("decr-key", -10, 100, 0);
        assertEquals(100L, (long) doc1.content());

        JsonLongDocument doc2 = bucket().counter("decr-key", -10, 0, 0);
        assertEquals(90L, (long) doc2.content());

        JsonLongDocument doc3 = bucket().counter("decr-key", -10, 0, 0);
        assertEquals(80L, (long) doc3.content());

        assertTrue(doc1.cas() != doc2.cas());
        assertTrue(doc1.cas() != doc3.cas());
        assertTrue(doc2.cas() != doc3.cas());
    }

    @Test
    public void shouldGetAndTouch() throws Exception {
        String id = "get-and-touch";

        JsonDocument upsert = bucket().upsert(JsonDocument.create(id, 3, JsonObject.empty().put("k", "v")));
        assertNotNull(upsert);
        assertEquals(id, upsert.id());

        Thread.sleep(2000);

        JsonDocument touched = bucket().getAndTouch(id, 3);
        assertEquals("v", touched.content().getString("k"));

        Thread.sleep(2000);

        touched = bucket().get(id);
        assertEquals("v", touched.content().getString("k"));
    }

    @Test
    public void shouldGetAndLock() throws Exception {
        String id = "get-and-lock";

        JsonDocument upsert = bucket().upsert(JsonDocument.create(id, JsonObject.empty().put("k", "v")));
        assertNotNull(upsert);
        assertEquals(id, upsert.id());

        JsonDocument locked = bucket().getAndLock(id, 2);
        assertEquals("v", locked.content().getString("k"));

        try {
            bucket().upsert(JsonDocument.create(id, JsonObject.empty().put("k", "v")));
            assertTrue(false);
        } catch(CASMismatchException ex) {
            assertTrue(true);
        }

        Thread.sleep(3000);

        bucket().upsert(JsonDocument.create(id, JsonObject.empty().put("k", "v")));
    }

    @Test
    public void shouldUnlock() throws Exception {
        String key = "unlock";

        JsonDocument upsert = bucket().upsert(JsonDocument.create(key, JsonObject.empty().put("k", "v")));
        assertNotNull(upsert);
        assertEquals(key, upsert.id());

        JsonDocument locked = bucket().getAndLock(key, 15);
        assertEquals("v", locked.content().getString("k"));

        try {
            bucket().upsert(JsonDocument.create(key, JsonObject.empty().put("k", "v")));
            assertTrue(false);
        } catch(CASMismatchException ex) {
            assertTrue(true);
        }

        boolean unlocked = bucket().unlock(key, locked.cas());
        assertTrue(unlocked);

        bucket().upsert(JsonDocument.create(key, JsonObject.empty().put("k", "v")));
    }

    @Test
    public void shouldTouch() throws Exception {
        String key = "touch";

        JsonDocument upsert = bucket().upsert(JsonDocument.create(key, 3, JsonObject.empty().put("k", "v")));

        Thread.sleep(2000);

        Boolean touched = bucket().touch(key, 3);
        assertTrue(touched);

        Thread.sleep(2000);

        JsonDocument loaded = bucket().get(key);
        assertEquals("v", loaded.content().getString("k"));
    }

    @Test
    public void shouldPersistToMaster() {
        String key = "persist-to-master";

        bucket().upsert(JsonDocument.create(key, JsonObject.empty().put("k", "v")),
            PersistTo.MASTER, ReplicateTo.NONE);
    }

    @Test
    public void shouldRemoveFromMaster() {
        String key = "remove-from-master";

        bucket().upsert(JsonDocument.create(key, JsonObject.empty().put("k", "v")),
            PersistTo.MASTER, ReplicateTo.NONE);

        bucket().remove(key, PersistTo.MASTER, ReplicateTo.NONE);
    }

    @Test
    public void shouldUpsertLegacyObjectDocument() {
        String id = "legacy-upsert";
        User user = new User("Michael");
        LegacyDocument doc = LegacyDocument.create(id, user);
        bucket().upsert(doc);

        LegacyDocument found = bucket().get(id, LegacyDocument.class);
        assertEquals(found.content().getClass(), user.getClass());
        assertEquals("Michael", ((User) found.content()).getFirstname());
    }

    @Test
    public void shouldAppendString() {
        String id ="append1";
        String value = "foo";

        LegacyDocument doc = LegacyDocument.create(id, value);
        bucket().upsert(doc);

        bucket().append(LegacyDocument.create(id, "bar"));

        LegacyDocument found = bucket().get(id, LegacyDocument.class);
        assertEquals("foobar", found.content());
    }

    @Test
    public void shouldPrependString() {
        String id ="prepend1";
        String value = "bar";

        LegacyDocument doc = LegacyDocument.create(id, value);
        bucket().upsert(doc);

        bucket().prepend(LegacyDocument.create(id, "foo"));

        LegacyDocument found = bucket().get(id, LegacyDocument.class);
        assertEquals("foobar", found.content());
    }

    @Test(expected = DocumentDoesNotExistException.class)
    public void shouldFailOnNonExistingAppend() {
        LegacyDocument doc = LegacyDocument.create("appendfail", "fail");
        bucket().append(doc);
    }

    @Test
    public void shouldStoreAndLoadRawJsonDocument() {
        String id = "jsonRaw";
        String content = "{\"foo\": 1234}";

        bucket().insert(RawJsonDocument.create(id, content));

        RawJsonDocument foundRaw = bucket().get(id, RawJsonDocument.class);
        assertEquals(content, foundRaw.content());

        JsonDocument foundParsed = bucket().get(id);
        assertEquals(1234, (int) foundParsed.content().getInt("foo"));
    }

    static class User implements Serializable {

        private final String firstname;

        User(String firstname) {
            this.firstname = firstname;
        }

        public String getFirstname() {
            return firstname;
        }
    }

}
