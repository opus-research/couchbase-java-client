/*
 * Copyright (C) 2016 Couchbase, Inc.
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

import static com.googlecode.catchexception.CatchException.caughtException;
import static com.googlecode.catchexception.CatchException.verifyException;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.subdoc.BadDeltaException;
import com.couchbase.client.java.error.subdoc.CannotInsertValueException;
import com.couchbase.client.java.error.subdoc.MultiMutationException;
import com.couchbase.client.java.error.subdoc.PathExistsException;
import com.couchbase.client.java.error.subdoc.PathInvalidException;
import com.couchbase.client.java.error.subdoc.PathMismatchException;
import com.couchbase.client.java.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.error.subdoc.SubDocumentException;
import com.couchbase.client.java.error.subdoc.ZeroDeltaException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.MutateInBuilder;
import com.couchbase.client.java.util.CouchbaseTestContext;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the sub-document API in {@link Bucket}.
 */
public class SubDocumentTest {

    private static CouchbaseTestContext ctx;
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(SubDocumentTest.class);

    private String key = "SubdocAPI";
    private JsonObject testJson;

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
    public void initData() {
        testJson = JsonObject.create()
                .put("sub", JsonObject.create().put("value", "original"))
                .put("boolean", true)
                .put("string", "someString")
                .put("int", 123)
                .put("array", JsonArray.from("1", 2, true));

        ctx.bucket().upsert(JsonDocument.create(key, testJson));
    }

    @AfterClass
    public static void disconnect() throws InterruptedException {
        ctx.destroyBucketAndDisconnect();
        ctx.disconnect();
    }

    @Test
    public void testFragmentCanBeAnEntity() {
        ctx.bucket()
                .mutateIn(key)
                .upsert("user", new KeyValueTest.User("frank"), false)
                .remove("sub")
                .remove("array")
                .remove("string")
                .remove("boolean")
                .doMutate();

        JsonObject expected = JsonObject.create()
                .put("user", JsonObject.create().put("firstname", "frank"))
                .put("int", 123);

        assertEquals(expected, ctx.bucket().get(key).content());
    }

    //=== GET and EXIST ===
    @Test
    public void testGetInPathTranscodesToCorrectClasses() {
        Object objectFragment = ctx.bucket().lookupIn(key)
                .get("sub").doLookup().content(0);

        Object intFragment = ctx.bucket().lookupIn(key)
                .get("int").doLookup().content(0);

        Object stringFragment = ctx.bucket().lookupIn(key)
                .get("string").doLookup().content(0);

        Object arrayFragment = ctx.bucket().lookupIn(key)
                .get("array").doLookup().content(0);

        Object booleanFragment = ctx.bucket().lookupIn(key)
                .get("boolean").doLookup().content(0);

        JsonObject jsonObjectFragment = ctx.bucket().lookupIn(key)
                .get("sub").doLookup().content(0, JsonObject.class);

        assertNotNull(objectFragment);
        assertTrue(objectFragment.getClass().getName(), objectFragment instanceof JsonObject);

        assertNotNull(intFragment);
        assertTrue(intFragment instanceof Integer);

        assertNotNull(stringFragment);
        assertTrue(stringFragment instanceof String);

        assertNotNull(arrayFragment);
        assertTrue(arrayFragment instanceof JsonArray);

        assertNotNull(booleanFragment);
        assertTrue(booleanFragment instanceof Boolean);

        assertNotNull(jsonObjectFragment);
        assertEquals(JsonObject.create().put("value", "original"), jsonObjectFragment);
    }

    @Test(expected = DocumentDoesNotExistException.class)
    public void testGetInOnUnknownDocumentThrowsException() {
        ctx.bucket().lookupIn("blabla").get("array").doLookup();
    }

    @Test
    public void testGetInUnknownPathReturnsContentNull() {
        DocumentFragment<Lookup> result = ctx.bucket().lookupIn(key).get("badPath").doLookup();

        assertNotNull(result);
        assertEquals(null, result.content(0));
        assertEquals(null, result.content("badPath"));
    }

    @Test(expected = PathMismatchException.class)
    public void testGetInPathMismatchThrowsException() {
        ctx.bucket().lookupIn(key).get("sub[1]").doLookup();
    }

    @Test
    public void testExistsIn() {
        DocumentFragment<Lookup> resultSub = ctx.bucket().lookupIn(key).exists("sub").doLookup();
        DocumentFragment<Lookup> resultInt = ctx.bucket().lookupIn(key).exists("int").doLookup();
        DocumentFragment<Lookup> resultString = ctx.bucket().lookupIn(key).exists("string").doLookup();
        DocumentFragment<Lookup> resultArray = ctx.bucket().lookupIn(key).exists("array").doLookup();
        DocumentFragment<Lookup> resultBoolean = ctx.bucket().lookupIn(key).exists("boolean").doLookup();

        assertTrue(resultSub.exists("sub"));
        assertTrue(resultInt.exists("int"));
        assertTrue(resultString.exists("string"));
        assertTrue(resultArray.exists("array"));
        assertTrue(resultBoolean.exists("boolean"));

        assertEquals(Boolean.TRUE, resultSub.content("sub"));
        assertEquals(Boolean.TRUE, resultInt.content("int"));
        assertEquals(Boolean.TRUE, resultString.content("string"));
        assertEquals(Boolean.TRUE, resultArray.content("array"));
        assertEquals(Boolean.TRUE, resultBoolean.content("boolean"));
    }

    @Test(expected = DocumentDoesNotExistException.class)
    public void testExistsInOnUnknownDocumentThrowsException() {
        ctx.bucket().lookupIn("blabla").exists("array").doLookup();
    }

    @Test
    public void testExistsInUnknownPathReturnContentFalse() {
        DocumentFragment<Lookup> result = ctx.bucket().lookupIn(key).exists("badPath").doLookup();

        assertNotNull(result);
        assertEquals(false, result.content(0));
        assertEquals(false, result.content("badPath"));
    }

    @Test(expected = PathMismatchException.class)
    public void testExistOnMismatchPathThrowsException() {
        ctx.bucket().lookupIn(key).exists("sub[1]").doLookup();
    }

    //=== Mutations with EMPTY path ===
    @Test
    public void testInsertEmptyPath() {
        verifyException(ctx.bucket().mutateIn(key), IllegalArgumentException.class)
                .insert("", "foo", false); //exception thrown as soon as the builder method is invoked
    }

    @Test
    public void testUpsertEmptyPath() {
        verifyException(ctx.bucket().mutateIn(key), IllegalArgumentException.class)
                .upsert("", "foo", false); //exception thrown as soon as the builder method is invoked
    }

    @Test
    public void testReplaceEmptyPath() {
        verifyException(ctx.bucket().mutateIn(key), IllegalArgumentException.class)
                .replace("", "foo"); //exception thrown as soon as the builder method is invoked
    }

    @Test
    public void testRemoveEmptyPath() {
        verifyException(ctx.bucket().mutateIn(key), IllegalArgumentException.class)
                .remove(""); //exception thrown as soon as the builder method is invoked
    }

    @Test
    public void testArrayInsertEmptyPath() {
        verifyException(ctx.bucket().mutateIn(key), IllegalArgumentException.class)
                .arrayInsert("", "foo"); //exception thrown as soon as the builder method is invoked
    }

    @Test
    public void testCounterEmptyPath() {
        verifyException(ctx.bucket().mutateIn(key), IllegalArgumentException.class)
                .counter("", 1000L, false); //exception thrown as soon as the builder method is invoked
    }

    //=== Mutation with BAD CAS ===
    @Test(expected = CASMismatchException.class)
    public void testUpsertInWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .upsert("int", null, false)
                .doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testInsertInWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .insert("int", null, false)
                .doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testReplaceInWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .replace( "int", null)
                .doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testExtendInFrontWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .pushFront("int", "something", false)
                .doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testExtendInBackWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .pushBack("int", "something", false)
                .doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testArrayInsertWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .arrayInsert("int", null)
                .doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testArrayAddUniqueWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .addUnique("int", null, false)
                .doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testRemoveWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .remove("int")
                .doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testCounterInWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .counter("int", 1000L, false)
                .doMutate();
    }

    //==== Durability and Expiry Litmus Tests ====
    @Test
    public void testSingleMutationWithDurability() {
        int numReplicas = ctx.bucketManager().info().replicaCount();
        int numNodes = ctx.bucketManager().info().nodeCount();
        Assume.assumeTrue("At least one available replica is necessary for this test", numReplicas >= 1 && numNodes >= (numReplicas + 1));

        int timeout = 10;
        PersistTo persistTo = PersistTo.MASTER;
        ReplicateTo replicateTo = ReplicateTo.ONE;

        //single mutations
        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                        .addUnique("array", "foo", false));

        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                        .arrayInsert("array[1]", "bar"));

        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                        .counter("int", 1000L, false));

        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                .pushFront("array", "extendFront", false));

        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                .pushBack("array", "extendBack", false));

        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                .insert("sub.insert", "inserted", false));

        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                .remove("boolean"));

        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                .replace("sub.value", "replaced"));

        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket().mutateIn(key)
                .upsert("newDict", JsonObject.create().put("value", 1), false));

        //assert final state of JSON
        JsonObject expected = JsonObject.create()
                .put("sub", JsonObject.create().put("value", "replaced").put("insert", "inserted"))
                .put("newDict", JsonObject.create().put("value", 1))
                .put("string", "someString")
                .put("int", 1123)
                .put("array", JsonArray.from("extendFront", "1", "bar", 2, true, "foo", "extendBack"));
        assertEquals(expected, ctx.bucket().get(key).content());
    }

    @Test
    public void testMultiMutationWithDurability() {
        int numReplicas = ctx.bucketManager().info().replicaCount();
        int numNodes = ctx.bucketManager().info().nodeCount();
        Assume.assumeTrue("At least one available replica is necessary for this test", numReplicas >= 1 && numNodes >= (numReplicas + 1));

        int timeout = 10;
        PersistTo persistTo = PersistTo.MASTER;
        ReplicateTo replicateTo = ReplicateTo.ONE;

        //multi mutations
        assertMutationReplicated(key, timeout, persistTo, replicateTo,
                ctx.bucket()
                .mutateIn(key)
                .addUnique("array", "foo", false)
                .remove("boolean"));

        //assert final state of JSON
        JsonObject expected = JsonObject.create()
                .put("sub", JsonObject.create().put("value", "original"))
                // removed
//              .put("boolean", true)
                .put("string", "someString")
                .put("int", 123)
                .put("array", JsonArray.from("1", 2, true, "foo"));
        assertEquals(expected, ctx.bucket().get(key).content());
    }

    private void assertMutationReplicated(String key, int timeout, PersistTo persistTo, ReplicateTo replicateTo,
            MutateInBuilder mutateInBuilder) {
        //ensure durability factors are set up
        mutateInBuilder.withDurability(persistTo, replicateTo);
        LOGGER.info("Asserting replication of {}", mutateInBuilder);

        DocumentFragment<Mutation> result = mutateInBuilder.doMutate(timeout, TimeUnit.SECONDS);

        JsonDocument masterDoc = ctx.bucket().get(key);
        JsonDocument replicaDoc = ctx.bucket().getFromReplica(key, ReplicaMode.FIRST).get(0);

        assertNotNull("result is null", result);
        assertEquals("master doc and fragment cas differ", masterDoc.cas(), result.cas());
        assertEquals("master doc and fragment mutation token differ", masterDoc.mutationToken(), result.mutationToken());

        assertEquals("replicated doc and fragment cas differ", replicaDoc.cas(), result.cas());
        assertEquals("replicated doc and fragment token differ", replicaDoc.mutationToken(), result.mutationToken());
        assertEquals("master doc and replicated doc contents differ", masterDoc.content(), replicaDoc.content());
    }

    private void assertMutationWithExpiry(String expiredKey, MutateInBuilder builder, int expirySeconds) throws InterruptedException {
        ctx.bucket().upsert(JsonDocument.create(expiredKey, testJson), PersistTo.MASTER);

        builder = builder.withExpiry(expirySeconds);
        LOGGER.info("Resetting expiry via {}", builder);
        DocumentFragment<Mutation> result = builder.doMutate();

        assertNotNull("mutation failed", result);
        assertNotNull("document has expired too soon", ctx.bucket().get(expiredKey));

        //then wait for at least 1s total to have passed since last operation, see that the document is gone
        Thread.sleep(expirySeconds * 1000 * 2);
        assertNull("document should have expired after last operation", ctx.bucket().get(expiredKey));
    }

    @Test
    public void testMutationWithExpirySingleCommonPath() throws InterruptedException {
        final String expiredKey = "SubdocMutationWithExpirySingleCommonPath";
        MutateInBuilder builder = ctx.bucket()
                .mutateIn(expiredKey)
                .addUnique("array", "foo", false);

        assertMutationWithExpiry(expiredKey, builder, 3);
    }

    @Test
    public void testMutationWithExpirySingleRemovePath() throws InterruptedException {
        final String expiredKey = "SubdocMutationWithExpirySingleRemovePath";
        MutateInBuilder builder = ctx.bucket()
                .mutateIn(expiredKey)
                .remove("boolean");

        assertMutationWithExpiry(expiredKey, builder, 1);
    }

    @Test
    public void testMutationWithExpirySingleCounterPath() throws InterruptedException {
        final String expiredKey = "SubdocMutationWithExpirySingleCounterPath";
        MutateInBuilder builder = ctx.bucket()
                .mutateIn(expiredKey)
                .counter("int", 1000L, false);

        assertMutationWithExpiry(expiredKey, builder, 1);
    }

    @Test
    public void testMutationWithExpiryMultiPath() throws InterruptedException {
        final String expiredKey = "SubdocMutationWithExpiryMultiPath";
        MutateInBuilder builder = ctx.bucket()
                .mutateIn(expiredKey)
                .upsert("newDict", "notADict", false)
                .remove("sub");

        assertMutationWithExpiry(expiredKey, builder, 1);
    }

    //=== UPSERT ===
    @Test
    public void testUpsertInDictionaryCreates() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .upsert("sub.newValue", "sValue", false)
                .doMutate();

        assertNotNull(result);
        assertEquals(ResponseStatus.SUCCESS, result.status(0));
        assertNotEquals(0L, result.cas());
        assertEquals("sValue", ctx.bucket().get(key).content().getObject("sub").getString("newValue"));
    }

    @Test
    public void testUpsertInDictionaryUpdates() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .upsert("sub.value", true, false)
                .doMutate();

        assertNotNull(result);
        assertEquals(ResponseStatus.SUCCESS, result.status(0));
        assertNotEquals(0L, result.cas());
        assertEquals(Boolean.TRUE, ctx.bucket().get(key).content().getObject("sub").getBoolean("value"));
    }

    @Test(expected = PathNotFoundException.class)
    public void testUpsertInDictionaryExtraLevelFails() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .upsert("sub.some.path", 1024, false)
                .doMutate();

        singleResult.content(0);
    }

    @Test
    public void testUpsertInDictionaryExtraLevelSucceedsWithCreatesParents() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .upsert("sub.some.path", 1024, true)
                .doMutate();

        assertNotNull(result);
        assertEquals(ResponseStatus.SUCCESS, result.status(0));
        assertNotEquals(0L, result.cas());
        int content = ctx.bucket().get(key).content().getObject("sub").getObject("some").getInt("path");
        assertEquals(1024, content);
    }

    @Test(expected = PathMismatchException.class)
    public void testUpsertInScalarFails() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .upsert("boolean.some", "string", false)
                .doMutate();

        singleResult.content(0);
    }

    @Test(expected = PathMismatchException.class)
    public void testUpsertInArrayFails() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .upsert("array.some", "string", false)
                .doMutate();

        singleResult.content(0);
    }

    @Test(expected = PathInvalidException.class)
    public void testUpsertInArrayIndexFails() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .upsert("array[1]", "string", false)
                .doMutate();

        singleResult.content(0);
    }


    //=== INSERT ===
    @Test
    public void testInsertInDictionaryCreates() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .insert("sub.newValue", "sValue", false)
                .doMutate();

        assertNotNull(result);
        assertEquals(ResponseStatus.SUCCESS, result.status(0));
        assertNotEquals(0L, result.cas());
        assertEquals("sValue", ctx.bucket().get(key).content().getObject("sub").getString("newValue"));
    }

    @Test(expected = PathExistsException.class)
    public void testInsertInDictionaryDoesntUpdate() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .insert("sub.value", true, false)
                .doMutate();

        result.content(0);
    }

    @Test(expected = PathNotFoundException.class)
    public void testInsertInDictionaryExtraLevelFails() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .insert("sub.some.path", 1024, false)
                .doMutate();

        result.content(0);
    }

    @Test
    public void testInsertInDictionaryExtraLevelSucceedsWithCreatesParents() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .insert("sub.some.path", 1024, true)
                .doMutate();

        assertNotNull(result);
        assertEquals(ResponseStatus.SUCCESS, result.status(0));
        assertNotEquals(0L, result.cas());
        int content = ctx.bucket().get(key).content().getObject("sub").getObject("some").getInt("path");
        assertEquals(1024, content);
    }

    @Test(expected = PathMismatchException.class)
    public void testInsertInScalarFails() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .insert("boolean.some", "string", false)
                .doMutate();

        result.content(0);
    }

    @Test(expected = PathMismatchException.class)
    public void testInsertInArrayFails() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .insert("array.some", "string", false)
                .doMutate();

        result.content(0);
    }

    @Test(expected = PathInvalidException.class)
    public void testInsertInArrayIndexFails() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .insert("array[1]", "string", false)
                .doMutate();

        result.content(0);
    }


    //=== REPLACE ===
    @Test(expected = PathNotFoundException.class)
    public void testReplaceInDictionaryDoesntCreate() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .replace("sub.newValue", "sValue")
                .doMutate();

        singleResult.content(0);
    }

    @Test
    public void testReplaceInDictionaryUpdates() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .replace("sub.value", true)
                .doMutate();

        assertNotNull(singleResult);
        assertEquals(ResponseStatus.SUCCESS, singleResult.status(0));
        assertNotEquals(0, singleResult.cas());
        assertEquals(Boolean.TRUE, ctx.bucket().get(key).content().getObject("sub").getBoolean("value"));
    }

    @Test(expected = PathMismatchException.class)
    public void testReplaceInScalarFails() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .replace("boolean.some", "string")
                .doMutate();

        singleResult.content(0);
    }

    @Test(expected = PathMismatchException.class)
    public void testReplaceInArrayFails() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .replace( "array.some", "string")
                .doMutate();

        singleResult.content(0);
    }

    @Test
    public void testReplaceInArrayIndexUpdates() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .replace("array[1]", "string")
                .doMutate();

        singleResult.content(0);

        assertNotNull(singleResult);
        assertEquals(ResponseStatus.SUCCESS, singleResult.status(0));
        assertNotEquals(0L, singleResult.cas());
        assertEquals("string", ctx.bucket().get(key).content().getArray("array").getString(1));
    }

    @Test(expected = PathNotFoundException.class)
    public void testReplaceInArrayIndexOutOfBoundsFails() {
        DocumentFragment<Mutation> singleResult = ctx.bucket()
                .mutateIn(key)
                .replace("array[3]", "badIndex")
                .doMutate();

        singleResult.content(0);
    }

    //=== EXTEND ===
    @Test(expected = PathMismatchException.class)
    public void testExtendOnNonArrayFails() {
        final String path = "sub";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .pushBack(path, "string", false)
                .doMutate();
    }

    @Test
    public void testExtendAtBackOfArray() {

        final String path = "array";
        final String value = "newElement";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .pushBack(path, value, false)
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        assertNull(result.content(0));
        JsonArray array = ctx.bucket().get(key).content().getArray(path);
        assertEquals(4, array.size());
        assertEquals(value, array.getString(3));
    }

    @Test
    public void testExtendAtFrontOfArray() {
        final String path = "array";
        final String value = "newElement";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .pushFront(path, value, false)
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        assertNull(result.content(0));
        JsonArray array = ctx.bucket().get(key).content().getArray(path);
        assertEquals(4, array.size());
        assertEquals(value, array.getString(0));
    }

    @Test
    public void testExtendInDictionaryWithCreateParentsCreatesArray() {
        final String path = "sub.array";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .pushFront(path, "newElement", true)
                .doMutate();

        assertNotNull(result);
        assertNull(result.content(0));
        assertNotEquals(0L, result.cas());
        JsonArray array = ctx.bucket().get(key).content().getObject("sub").getArray("array");
        assertEquals(1, array.size());
        assertEquals("newElement", array.getString(0));
    }

    @Test(expected = PathNotFoundException.class)
    public void testExtendInDictionnaryWithoutCreateParentsFails() {
        final String path = "sub.array";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .pushFront(path, "newElement", false)
                .doMutate();
    }

    @Test
    public void testExtendAtBackOfRootArrayWorks() {
        String arrayKey = "subdocArray";
        String path = "";
        final String value1 = "unique";
        final String value2 = "back";
        ctx.bucket().upsert(JsonArrayDocument.create(arrayKey, JsonArray.empty()));

        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(arrayKey)
                .pushBack(path, value1, false)
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        assertNull(result.content(0));
        JsonArray array = ctx.bucket().get(arrayKey, JsonArrayDocument.class).content();
        assertEquals(1, array.size());
        assertEquals(value1, array.getString(0));

        DocumentFragment<Mutation> result2 = ctx.bucket()
                .mutateIn(arrayKey)
                .pushBack(path, value2, false)
                .doMutate();

        assertNotNull(result2);
        assertNotEquals(0L, result2.cas());
        assertNull(result.content(0));
        array = ctx.bucket().get(arrayKey, JsonArrayDocument.class).content();
        assertEquals(2, array.size());
        assertEquals(value1, array.getString(0));
        assertEquals(value2, array.getString(1));
    }

    @Test
    public void testExtendAtFrontOfRootArrayWorks() {
        String arrayKey = "subdocArray";
        String path = "";
        final String value1 = "unique";
        final String value2 = "front";
        ctx.bucket().upsert(JsonArrayDocument.create(arrayKey, JsonArray.empty()));

        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(arrayKey)
                .pushFront(path, value1, true)
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        assertNull(result.content(0));
        JsonArray array = ctx.bucket().get(arrayKey, JsonArrayDocument.class).content();
        assertEquals(1, array.size());
        assertEquals(value1, array.getString(0));

        DocumentFragment<Mutation> result2 = ctx.bucket()
                .mutateIn(arrayKey)
                .pushFront(path, value2, true)
                .doMutate();

        assertNotNull(result2);
        assertNotEquals(result.cas(), result2.cas());
        array = ctx.bucket().get(arrayKey, JsonArrayDocument.class).content();
        assertEquals(2, array.size());
        assertEquals(value2, array.getString(0));
        assertEquals(value1, array.getString(1));
    }


    //=== ARRAY INSERT ===
    @Test
    public void testArrayInsertAtIndexZero() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .arrayInsert("array[0]", "arrayInsert")
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(4, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(0));
    }

    @Test
    public void testArrayInsertAtSize() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .arrayInsert("array[3]", "arrayInsert")
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(4, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(3));
    }

    @Test
    public void testArrayInsertAtIndexZeroOnEmptyArray() {
        //prepare doc with empty array
        JsonObject withEmptyArray = JsonObject.create().put("array", JsonArray.empty());
        ctx.bucket().upsert(JsonDocument.create(key, withEmptyArray));

        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .arrayInsert("array[0]", "arrayInsert")
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(1, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(0));
    }

    @Test
    public void testArrayInsertAtExistingIndex() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .arrayInsert("array[1]", "arrayInsert")
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(4, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(1));
        assertEquals(2, storedArray.getInt(2).intValue());
        assertEquals(true, storedArray.getBoolean(3));
    }

    @Test(expected = PathNotFoundException.class)
    public void testArrayInsertAtIndexOutOfBounds() {
        final String path = "array[5]";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .arrayInsert(path, "arrayInsert")
                .doMutate();
    }

    @Test(expected = PathInvalidException.class)
    public void testArrayInsertAtNegativeIndex() {
        final String path = "array[-1]";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .arrayInsert(path, "arrayInsert")
                .doMutate();
    }

    @Test(expected = PathNotFoundException.class)
    public void testArrayInsertOnArrayThatDoesntExist() {
        final String path = "secondArray[0]";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .arrayInsert(path, "arrayInsert")
                .doMutate();
    }

    @Test(expected = PathInvalidException.class)
    public void testArrayInsertOnPathNotEndingWithArrayElement() {
        final String path = "array";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .arrayInsert(path, "arrayInsert")
                .doMutate();
    }


    //=== ARRAY ADD UNIQUE ===
    @Test(expected = PathMismatchException.class)
    public void testArrayAddUniqueInNonArray() {
        final String path = "sub";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .addUnique(path, "arrayInsert", false)
                .doMutate();
    }

    @Test(expected = PathMismatchException.class)
    public void testArrayAddUniqueInArrayWithNonPrimitives() {
        //create document with array containing array
        JsonObject root = JsonObject.create().put("array", JsonArray.create().add(JsonArray.empty()));
        ctx.bucket().upsert(JsonDocument.create(key, root));

        //not a primitive only array => MISMATCH
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .addUnique("array", "arrayInsert", false)
                .doMutate();
    }

    @Test(expected = CannotInsertValueException.class)
    public void testArrayAddUniqueWithNonPrimitiveFragment() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .addUnique("array", JsonObject.create().put("object", true), false)
                .doMutate();
    }

    @Test(expected = PathExistsException.class)
    public void testArrayAddUniqueWithValueAlreadyPresent() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .addUnique("array", true, false)
                .doMutate();
    }

    @Test(expected = PathNotFoundException.class)
    public void testArrayAddUniqueOnNonExistingArray() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .addUnique("anotherArray", "arrayInsert", false)
                .doMutate();
    }

    @Test
    public void testArrayAddUniqueOnNonExistingArraySucceedsWithCreateParents() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .addUnique( "anotherArray", "arrayInsert", true)
                .doMutate();

        assertNotNull(result);
        assertNotEquals(0L, result.cas());
        assertNull(result.content(0));
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("anotherArray");
        assertEquals(1, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(0));
    }


    //=== REMOVE ===
    @Test
    public void testRemoveScalar() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .remove("int")
                .doMutate();

        assertNotNull(result);
        assertNull(result.content("int"));
        assertEquals(ResponseStatus.SUCCESS, result.status(0));
        assertNotEquals(0L, result.cas());
        assertFalse(ctx.bucket().get(key).content().containsKey("int"));
    }

    @Test
    public void testRemoveDictEntry() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .remove("sub.value")
                .doMutate();

        assertNotNull(result);
        assertNull(result.content("sub.value"));
        assertEquals(ResponseStatus.SUCCESS, result.status(0));
        assertNotEquals(0L, result.cas());
        assertEquals(0, ctx.bucket().get(key).content().getObject("sub").size());
    }

    @Test
    public void testRemoveArrayElement() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .remove("array[1]")
                .doMutate();

        assertNotNull(result);
        assertNull(result.content("array[1]"));
        assertEquals(ResponseStatus.SUCCESS, result.status(0));
        assertNotEquals(0L, result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(2, storedArray.size());
        assertEquals("1", storedArray.getString(0));
        assertEquals(true, storedArray.getBoolean(1));
    }

    @Test
    public void testRemoveLastItem() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .remove("array[-1]")
                .doMutate();

        assertNotNull(result);
        assertNull(result.content("array[-1]"));
        assertEquals(ResponseStatus.SUCCESS, result.status("array[-1]"));
        assertNotEquals(0L, result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(2, storedArray.size());
        assertEquals("1", storedArray.getString(0));
        assertEquals(2, storedArray.getInt(1).intValue());
    }

    @Test(expected = PathNotFoundException.class)
    public void testRemoveScalarWithBadPath() {
        String path = "integer";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .remove(path)
                .doMutate();
    }

    @Test(expected = PathNotFoundException.class)
    public void testRemoveDictEntryWithBadKey() {
        String path = "sub.valuezz";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .remove(path)
                .doMutate();
    }

    @Test(expected = PathNotFoundException.class)
    public void testRemoveArrayElementWithIndexOutOfBounds() {
        final String path = "array[4]";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .remove(path)
                .doMutate();
    }

    //=== COUNTER ===
    @Test
    public void testCounterWithPositiveDeltaIncrements() {
        String path = "int";
        long delta = 1000L;
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter(path, delta, false)
                .doMutate();

        assertThat(result.content(path), instanceOf(Long.class));
        assertEquals(1123L, result.content(path, Long.class).longValue());
        assertEquals(1123L, ctx.bucket().get(key).content().getLong(path).longValue());
    }

    @Test
    public void testCounterWithNegativeDeltaDecrements() {
        String path = "int";
        long delta = -123L;
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter(path, delta, false)
                .doMutate();

        assertThat(result.content(path), instanceOf(Long.class));
        assertEquals(0L, result.content(path, Long.class).longValue());
        assertEquals(0L, ctx.bucket().get(key).content().getLong("int").longValue());
    }

    @Test(expected = ZeroDeltaException.class)
    public void testCounterWithZeroDeltaFails() {
        ctx.bucket()
                .mutateIn(key)
                .counter("int", 0L, false); //fails fast
    }

    //TODO is there a way of testing for NumberTooBigException (the stored number would have to be greater than Long.MAX.VALUE)

    @Test
    public void testCounterProducingTooLargeValueFails() {
        System.out.println(ctx.bucket().get(key));
        String path = "int";
        long delta = Long.MAX_VALUE - 123L;

        //first increment should work
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter(path, delta, false)
                .doMutate();

        assertNotNull(result);
        assertEquals(ResponseStatus.SUCCESS, result.status(path));
        assertEquals(Long.MAX_VALUE, result.content(0, Long.class).longValue());
        assertEquals(Long.MAX_VALUE, result.content(path, Long.class).longValue());

        //second increment should fail, as a subdoc level error
        verifyException(ctx.bucket()
                .mutateIn(key)
                .counter(path, delta, false))
                .doMutate();
        assertThat("second counter increment should have made the counter value too big",
                caughtException(), instanceOf(BadDeltaException.class));
    }

    @Test
    public void testCounterInPartialPathMissingLastPathElementCreatesNewCounter() {
        final String path = "sub.counter";
        final long delta = 1000L;
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter(path, delta, false)
                .doMutate();

        assertThat(result.content(path), instanceOf(Long.class));
        assertEquals(1000L, result.content(path, Long.class).longValue());
        assertEquals(1000L, result.content(0, Long.class).longValue());
        assertEquals(1000L, ctx.bucket().get(key).content().getObject("sub").getLong("counter").longValue());
    }

    @Test
    public void testCounterDeltaUpperBoundIsLongMaxValue() {
        long expected = Long.MAX_VALUE;
        final String path = "newCounter";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter(path, expected, false)
                .doMutate();

        assertThat(result.content(path), instanceOf(Long.class));
        assertEquals(expected, result.content(path, Long.class).longValue());
        assertEquals(expected, result.content(0, Long.class).longValue());
        assertEquals(expected, ctx.bucket().get(key).content().getLong(path).longValue());
    }

    @Test
    public void testCounterWithLongMinValueDeltaSucceedsOnNewCounter() {
        long expected = Long.MIN_VALUE + 1L;
        final String path = "newCounter";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter(path, expected, false)
                .doMutate();

        assertThat(result.content(path), instanceOf(Long.class));
        assertEquals(expected, result.content(path, Long.class).longValue());
        assertEquals(expected, result.content(0, Long.class).longValue());
        assertEquals(expected, ctx.bucket().get(key).content().getLong(path).longValue());
    }

    @Test(expected = PathMismatchException.class)
    public void testCounterOnNonNumericPathFails() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter("sub.value", 1000L, false)
                .doMutate();
    }

    @Test(expected = PathNotFoundException.class)
    public void testCounterInPartialPathMissingIntermediaryElementFails() {
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter("counters.a", 1000L, false)
                .doMutate();
    }

    @Test
    public void testCounterInPartialPathMissingIntermediaryElementWithCreateParentsSucceeds() {
        long delta = 1000L;
        final String path = "counters.a";
        DocumentFragment<Mutation> result = ctx.bucket()
                .mutateIn(key)
                .counter(path, delta, true)
                .doMutate();

        assertThat(result.content(path), instanceOf(Long.class));
        assertEquals(delta, result.content(path, Long.class).longValue());
        assertEquals(delta, result.content(0, Long.class).longValue());
        assertEquals(delta, ctx.bucket().get(key).content().getObject("counters").getLong("a").longValue());
    }

    //=== MULTI LOOKUP ===

    @Test(expected = IllegalArgumentException.class)
    public void testMultiLookupEmptySpecFails() {
        ctx.bucket().lookupIn(key).doLookup();
    }

    @Test
    public void testMultiLookup() {
        DocumentFragment<Lookup> results = ctx.bucket()
                .lookupIn(key)
                .get("boolean")
                .get("sub")
                .exists("string")
                //path not found => content null/false
                .get("no")
                .exists("no")
                //other error => content throws
                .get("sub[1]")
                .exists("sub[1]")
                .doLookup();

        assertNotNull(results);
        assertEquals(7, results.size());

        //assert the paths exist and didn't fail (ie we can call content() on them without an exception)
        assertEquals(true, results.exists(0));
        assertEquals(true, results.exists(1));
        assertEquals(true, results.exists(2));
        assertEquals(true, results.exists(3));
        assertEquals(true, results.exists(4));
        assertEquals(false, results.exists(5));
        assertEquals(false, results.exists(6));

        //assert the type of value that content() returns
        assertTrue(results.content(0) instanceof Boolean);
        assertTrue(results.content(1) instanceof JsonObject);
        assertTrue(results.content(2) instanceof Boolean);
        assertEquals(null, results.content(3));
        assertEquals(false, results.content(4));
        verifyException(results).content(5);
        assertThat("expected subdocument exception when getting content for #5",
                caughtException(), instanceOf(SubDocumentException.class));
        verifyException(results).content(6);
        assertThat("expected subdocument exception when getting content for #6",
                caughtException(), instanceOf(SubDocumentException.class));
    }

    @Test
    public void testMultiLookupExistDoesNotFailOnBadPath() {
        String path1 = "sub[1]";
        String path2 = "badPath";
        String path3 = "sub";
        DocumentFragment<Lookup> results = ctx.bucket()
                .lookupIn(key)
                .exists(path1)
                .exists(path2)
                .exists(path3)
                .doLookup();

        assertNotNull(results);
        assertEquals(3, results.size());

        //errors throw an exception when calling content
        assertFalse(results.exists(0));
        assertEquals(ResponseStatus.SUBDOC_PATH_MISMATCH, results.status(0));
        verifyException(results, PathMismatchException.class).content(0);
        assertFalse(results.exists(path1));
        assertEquals(ResponseStatus.SUBDOC_PATH_MISMATCH, results.status(path1));
        verifyException(results, PathMismatchException.class).content(path1);

        //except path not found gives a content, false
        assertTrue(results.exists(1)); //means that content() won't throw
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, results.status(1));
        assertEquals(false, results.content(1));
        assertTrue(results.exists(path2)); //means that content() won't throw
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, results.status(path2));
        assertEquals(false, results.content(path2));

        //success gives a content, true
        assertTrue(results.exists(2));
        assertEquals(true, results.content(2));
        assertEquals(ResponseStatus.SUCCESS, results.status(2));
        assertTrue(results.exists(path3));
        assertEquals(true, results.content(path3));
        assertEquals(ResponseStatus.SUCCESS, results.status(path3));
    }

    @Test
    public void testMultiLookupGetDoesNotFailOnBadPath() {
        DocumentFragment<Lookup> results = ctx.bucket()
                .lookupIn(key)
                .get("sub")
                .get("sub[1]")
                .get("badPath")
                .doLookup();

        assertNotNull(results);
        assertEquals(3, results.size());

        assertNotNull(results.content(0));
        assertTrue(results.exists(0));
        assertEquals(ResponseStatus.SUCCESS, results.status(0));
        assertEquals(testJson.getObject("sub"), results.content(0));

        assertFalse(results.exists(1));
        assertEquals(ResponseStatus.SUBDOC_PATH_MISMATCH, results.status(1));
        verifyException(results, PathMismatchException.class).content(1);

        assertTrue(results.exists(2));
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, results.status(2));
        assertEquals(null, results.content(2));
    }

    //=== MULTI MUTATION ===
    @Test
    public void testMultiMutation() {
        DocumentFragment<Mutation> mmr = ctx.bucket()
                .mutateIn(key)
                .replace("sub.value", "replaced")
                .replace("string", "otherString")
                .upsert("sub.otherValue", "newValue", false)
                .arrayInsert("array[1]", "v")
                .addUnique("array", "v2", false)
                .pushBack("array", "v3", false)
                .counter("int", 1000, false)
                .insert("sub.insert", "inserted", false)
                .remove("boolean")
                .doMutate();

        JsonDocument stored = ctx.bucket().get(key);

        assertNotNull(mmr);
        assertNotEquals(0L, mmr.cas());
        assertEquals(stored.cas(), mmr.cas());
        assertEquals(stored.mutationToken(), mmr.mutationToken());

        assertEquals("replaced", stored.content().getObject("sub").getString("value"));
        assertEquals("otherString", stored.content().getString("string"));
        assertEquals("newValue", stored.content().getObject("sub").getString("otherValue"));
        assertEquals(JsonArray.from("1", "v", 2, true, "v2", "v3"), stored.content().getArray("array"));
        assertEquals(1123, stored.content().getInt("int").intValue());
        assertEquals("inserted", stored.content().getObject("sub").getString("insert"));
        assertFalse(stored.content().containsKey("boolean"));
    }

    @Test
    public void testMultiMutationWithCreateParents() {
        DocumentFragment<Mutation> mmr = ctx.bucket()
                .mutateIn(key)
                .addUnique("addUnique.array", "v", true)
                .counter("counter.newCounter", 100, true)
                .pushFront("extend.array", "v", true)
                .insert("insert.sub.entry", "v", true)
                .upsert("upsert.sub.entry", "v", true)
                .doMutate();

        JsonDocument stored = ctx.bucket().get(key);

        assertNotNull(mmr);
        assertNotEquals(0L, mmr.cas());
        assertEquals(stored.cas(), mmr.cas());
        assertEquals(stored.mutationToken(), mmr.mutationToken());

        assertEquals("v", stored.content().getObject("addUnique").getArray("array").getString(0));
        assertEquals(100L, stored.content().getObject("counter").getLong("newCounter").longValue());
        assertEquals("v", stored.content().getObject("extend").getArray("array").getString(0));
        assertEquals("v", stored.content().getObject("insert").getObject("sub").getString("entry"));
        assertEquals("v", stored.content().getObject("upsert").getObject("sub").getString("entry"));
    }

    @Test
    public void testMultiMutationWithFailure() {
        verifyException(ctx.bucket()
                .mutateIn(key)
                .replace("sub.value", "replaced")
                .replace("int", 1024)
                .upsert("sub.otherValue.deeper", "newValue", false)
                .replace("secondError", "unreachable"))
                .doMutate();

        assertThat(caughtException(), instanceOf(MultiMutationException.class));
        MultiMutationException e = caughtException();
        assertEquals(2, e.firstFailureIndex());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, e.firstFailureStatus());
        assertNotNull(e.getCause());
        assertTrue(e.getCause().toString(), e.getCause() instanceof  PathNotFoundException);
        assertTrue(e.getCause().toString(), e.getCause().toString().contains("sub.otherValue.deeper"));
        assertEquals(4, e.originalSpec().size());

        assertEquals(testJson, ctx.bucket().get(key).content());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultiMutationWithEmptySpecFails() {
        ctx.bucket().mutateIn(key).doMutate();
    }

    @Test(expected = CASMismatchException.class)
    public void testMultiMutationWithBadCas() {
        ctx.bucket()
                .mutateIn(key)
                .withCas(1234L)
                .replace("sub", 123)
                .remove("int")
        .doMutate();
    }
}