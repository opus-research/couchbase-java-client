package com.couchbase.client.java;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.NoSuchElementException;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.util.CouchbaseTestContext;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import org.junit.*;

public class DataStructuresTest {

    private static CouchbaseTestContext ctx;
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DataStructuresTest.class);


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
        ctx.bucket().async().listPush("dslist", 1).toBlocking().single();
        ctx.bucket().async().setAdd("dsset", 1).toBlocking().single();
        ctx.bucket().async().queueAdd("dsqueue", 1).toBlocking().single();
    }

    @AfterClass
    public static void disconnect() throws InterruptedException {
        ctx.destroyBucketAndDisconnect();
        ctx.disconnect();
    }

    @After
    public void cleanup() throws Exception {
        ctx.bucket().async().remove("dsmap").toBlocking().single();
        ctx.bucket().async().remove("dslist").toBlocking().single();
        ctx.bucket().async().remove("dsset").toBlocking().single();
        ctx.bucket().async().remove("dsqueue").toBlocking().single();
    }


    @Test
    public void testMap() {
        ctx.bucket().async().mapAdd("dsmap", "foo", "bar").toBlocking().single();
        String myval = ctx.bucket().async().mapGet("dsmap", "foo", String.class).toBlocking().single();
        assertEquals(myval, "bar");
        boolean result = ctx.bucket().async().mapRemove("dsmap", "foo").toBlocking().single();
        assertEquals(result, true);
        result = ctx.bucket().async().mapRemove("dsmap", "foo").toBlocking().single();
        assertEquals(result, false);
        int size = ctx.bucket().async().mapSize("dsmap").toBlocking().single();
        ctx.bucket().async().mapAdd("dsmap", "foo", "bar").toBlocking().single();
        int newSize = ctx.bucket().async().mapSize("dsmap").toBlocking().single();
        assert (newSize == size + 1);
    }

    @Test(expected = NoSuchElementException.class)
    public void testMapGetInvalidKey() {
        ctx.bucket().async().mapGet("dsmap", "9999", String.class).toBlocking().single();
    }

    @Test
    public void testList() {
        ctx.bucket().async().listPush("dslist", "abc").toBlocking().single();
        String myval = ctx.bucket().async().listGet("dslist", 1, String.class).toBlocking().single();
        assertEquals(myval, "abc");
        ctx.bucket().async().listShift("dslist", null).toBlocking().single();
        assertNull(ctx.bucket().async().listGet("dslist", 0, Object.class).toBlocking().single());
        ctx.bucket().async().listSet("dslist", 1, JsonArray.create().add("random")).toBlocking().single();
        JsonArray array = ctx.bucket().async().listGet("dslist", 1, JsonArray.class).toBlocking().single();
        assertEquals(array.get(0), "random");
        int size = ctx.bucket().async().listSize("dslist").toBlocking().single();
        assert (size > 0);
        ctx.bucket().async().listRemove("dslist", 1).toBlocking().single();
        int newSize = ctx.bucket().async().listSize("dslist").toBlocking().single();
        assertEquals(size, newSize + 1);
        ctx.bucket().async().listRemove("dslist", 1).toBlocking().single();
        ctx.bucket().async().listRemove("dslist", 1).toBlocking().single();
        ctx.bucket().async().listRemove("dslist", 0).toBlocking().single();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testListGetInvalidIndex() {
        ctx.bucket().async().listGet("dslist", 99999, Object.class).toBlocking().single();
    }


    @Test
    public void testQueue() {
        ctx.bucket().async().queueRemove("dsqueue", String.class).toBlocking().single();
        ctx.bucket().async().queueAdd("dsqueue", "val").toBlocking().single();
        String val = ctx.bucket().async().queueRemove("dsqueue", String.class).toBlocking().single();
        assertEquals(val, "val");
        ctx.bucket().async().queueAdd("dsqueue", null).toBlocking().single();
        assertNull(ctx.bucket().async().queueRemove("dsqueue", null).toBlocking().single());
    }


    @Test
    public void testSet() {
        boolean result = ctx.bucket().async().setAdd("dsset", "val").toBlocking().single();
        assertEquals(result, true);
        result = ctx.bucket().async().setAdd("dsset", "val").toBlocking().single();
        assertEquals(result, false);
        String val = ctx.bucket().async().setRemove("dsset", "val").toBlocking().single();
        assertEquals(val, "val");
        result = ctx.bucket().async().setAdd("dsset", "val").toBlocking().single();
        assertEquals(result, true);
        result = ctx.bucket().async().setAdd("dsset", null).toBlocking().single();
        assertEquals(result, true);
        assertNull(ctx.bucket().async().setRemove("dsset", null).toBlocking().single());
        result = ctx.bucket().async().setAdd("dsset", 2).toBlocking().single();
        assertEquals(result, true);
        result = ctx.bucket().async().setAdd("dsset", 2).toBlocking().single();
        assertEquals(result, false);
    }
}