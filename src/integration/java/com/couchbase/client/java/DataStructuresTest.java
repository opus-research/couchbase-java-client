/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java;

import static org.junit.Assert.*;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.datastructures.MutationOptionBuilder;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.util.CouchbaseTestContext;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import org.junit.*;

public class DataStructuresTest {

    private static CouchbaseTestContext ctx;

    @BeforeClass
    public static void connect() throws Exception {
        ctx = CouchbaseTestContext.builder()
                .bucketQuota(100)
                .bucketReplicas(1)
                .bucketType(BucketType.COUCHBASE)
                .build();

        ctx.ignoreIfMissing(CouchbaseFeature.SUBDOC);
    }

    @Before
    public void init() throws Exception {
        ctx.bucket().async().mapAdd("dsmap", "1", "1").toBlocking().single();
        ctx.bucket().async().mapAdd("dsmapFull", "1", "1").toBlocking().single();
        ctx.bucket().async().listPush("dslist", 1).toBlocking().single();
        ctx.bucket().async().listPush("dslistFull", 1).toBlocking().single();
    }

    @AfterClass
    public static void disconnect() throws InterruptedException {
        ctx.destroyBucketAndDisconnect();
        ctx.disconnect();
    }

    @After
    public void cleanup() throws Exception {
        ctx.bucket().async().remove("dsmap").toBlocking().single();
        ctx.bucket().async().remove("dsmapFull").toBlocking().single();
        ctx.bucket().async().remove("dslist").toBlocking().single();
        ctx.bucket().async().remove("dslistFull").toBlocking().single();
    }

    @Test
    public void testMap() {
        ctx.bucket().async().mapAdd("dsmap", "foo", "bar").toBlocking().single();
        String myval = ctx.bucket().async().mapGet("dsmap", "foo", String.class).toBlocking().single();
        assertEquals(myval, "bar");
        boolean result = ctx.bucket().async().mapRemove("dsmap", "foo").toBlocking().single();
        assertEquals(result, true);
        result = ctx.bucket().async().mapRemove("dsmap", "foo").toBlocking().single();
        assertEquals(result, true);
        int size = ctx.bucket().async().mapSize("dsmap").toBlocking().single();
        ctx.bucket().async().mapAdd("dsmap", "foo", "bar", MutationOptionBuilder.build().withDurability(PersistTo.MASTER)).toBlocking().single();
        int newSize = ctx.bucket().async().mapSize("dsmap").toBlocking().single();
        assert (newSize == size + 1);
        result = ctx.bucket().async().mapAdd("dsmap", "foo", null).toBlocking().single();
        assertEquals(result, true);
        result = ctx.bucket().async().mapAdd("dsmap", "foo", 10).toBlocking().single();
        assertEquals(result, true);
    }

    @Test(expected = NoSuchElementException.class)
    public void testMapGetInvalidKey() {
        ctx.bucket().async().mapGet("dsmap", "9999", String.class).toBlocking().single();
    }

    @Test(expected = CASMismatchException.class)
    public void testMapSetCasMismatch() {
        JsonDocument document = ctx.bucket().get("dsmap");
        ctx.bucket().async().mapAdd("dsmap", "foo", "bar", MutationOptionBuilder.build().withCAS(document.cas() + 1)).toBlocking().single();
    }

    @Test(expected = IllegalStateException.class)
    public void testMapFullException() {
        for (int i = 0; i < 5; i++) {
            char[] data = new char[5000000];
            String str = new String(data);
            boolean result = ctx.bucket().async().mapAdd("dsmapFull", "foo" + i, str, MutationOptionBuilder.build().withDurability(PersistTo.MASTER)).toBlocking().single();
            assertEquals(result, true);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testSyncMapAddTimeout() {
        ctx.bucket().mapAdd("dsmap", "timeout", "timeout", 1, TimeUnit.NANOSECONDS);
    }

    @Test
    public void testList() {
        ctx.bucket().async().listPush("dslist", "foo").toBlocking().single();
        int size = ctx.bucket().async().listSize("dslist").toBlocking().single();
        String myval = ctx.bucket().async().listGet("dslist", size - 1, String.class).toBlocking().single();
        assertEquals(myval, "foo");
        ctx.bucket().async().listShift("dslist", null).toBlocking().single();
        assertNull(ctx.bucket().async().listGet("dslist", 0, Object.class).toBlocking().single());
        ctx.bucket().async().listSet("dslist", 1, JsonArray.create().add("baz")).toBlocking().single();
        JsonArray array = ctx.bucket().async().listGet("dslist", 1, JsonArray.class).toBlocking().single();
        assertEquals(array.get(0), "baz");
        ctx.bucket().async().listSet("dslist", 1, JsonObject.create().put("foo", "bar")).toBlocking().single();
        JsonObject object = ctx.bucket().async().listGet("dslist", 1, JsonObject.class).toBlocking().single();
        assertEquals(object.get("foo"), "bar");
        size = ctx.bucket().async().listSize("dslist").toBlocking().single();
        assert (size > 0);
        ctx.bucket().async().listRemove("dslist", 1).toBlocking().single();
        int newSize = ctx.bucket().async().listSize("dslist").toBlocking().single();
        assertEquals(size - 1, newSize);
        while (newSize > 1) {
            ctx.bucket().async().listRemove("dslist", 1).toBlocking().single();
            newSize = ctx.bucket().async().listSize("dslist").toBlocking().single();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testListGetInvalidIndex() {
        ctx.bucket().async().listGet("dslist", -99999, Object.class).toBlocking().single();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testListSetInvalidIndex() {
        ctx.bucket().async().listSet("dslist", -10, "bar").toBlocking().single();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testListRemoveInvalidIndex() {
        ctx.bucket().async().listGet("dslist", -99999, Object.class).toBlocking().single();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testListRemoveNonExistentIndex() {
        ctx.bucket().async().listGet("dslist", 2, Object.class).toBlocking().single();
    }

    @Test(expected = IllegalStateException.class)
    public void testListPushFullException() {
        for (int i = 0; i < 5; i++) {
            char[] data = new char[5000000];
            String str = new String(data);
            boolean result = ctx.bucket().async().listPush("dslistFull", str, MutationOptionBuilder.build().withDurability(PersistTo.MASTER)).toBlocking().single();
            assertEquals(result, true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testListShiftFullException() {
        for (int i = 0; i < 5; i++) {
            char[] data = new char[5000000];
            String str = new String(data);
            boolean result = ctx.bucket().async().listShift("dslistFull", str, MutationOptionBuilder.build().withDurability(PersistTo.MASTER)).toBlocking().single();
            assertEquals(result, true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testListSetFullException() {
        for (int i = 0; i < 5; i++) {
            char[] data = new char[5000000];
            String str = new String(data);
            boolean result = ctx.bucket().async().listSet("dslistFull", 0, str, MutationOptionBuilder.build().withDurability(PersistTo.MASTER)).toBlocking().single();
            assertEquals(result, true);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testSyncListAddTimeout() {
        ctx.bucket().listPush("dslist", "timeout", 1, TimeUnit.NANOSECONDS);
    }
}