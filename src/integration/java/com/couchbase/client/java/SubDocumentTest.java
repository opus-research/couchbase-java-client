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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.subdoc.DocumentFragment;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.subdoc.ExtendDirection;
import com.couchbase.client.java.document.subdoc.LookupResult;
import com.couchbase.client.java.document.subdoc.LookupSpec;
import com.couchbase.client.java.document.subdoc.MutationSpec;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.subdoc.CannotInsertValueException;
import com.couchbase.client.java.error.subdoc.DeltaTooBigException;
import com.couchbase.client.java.error.subdoc.MultiMutationException;
import com.couchbase.client.java.error.subdoc.PathExistsException;
import com.couchbase.client.java.error.subdoc.PathInvalidException;
import com.couchbase.client.java.error.subdoc.PathMismatchException;
import com.couchbase.client.java.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.error.subdoc.ZeroDeltaException;
import com.couchbase.client.java.query.dsl.path.Path;
import com.couchbase.client.java.util.CouchbaseTestContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the sub-document API in {@link Bucket}.
 */
public class SubDocumentTest {

    private static CouchbaseTestContext ctx;

    private String key = "SubdocAPI";
    private JsonObject testJson;

    @BeforeClass
    public static void connect() throws Exception {
        ctx = CouchbaseTestContext.builder()
                .bucketQuota(256)
                .bucketType(BucketType.COUCHBASE)
                .flushOnInit(true)
                .build();
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
        ctx.disconnect();
    }

    //=== GET and EXIST ===

    @Test
    public void testGetInPathTranscodesToCorrectClasses() {
        DocumentFragment<Object> objectFragment = ctx.bucket().getIn(key, "sub", Object.class);
        DocumentFragment<Object> intFragment = ctx.bucket().getIn(key, "int", Object.class);
        DocumentFragment<Object> stringFragment = ctx.bucket().getIn(key, "string", Object.class);
        DocumentFragment<Object> arrayFragment = ctx.bucket().getIn(key, "array", Object.class);
        DocumentFragment<Object> booleanFragment = ctx.bucket().getIn(key, "boolean", Object.class);

        assertNotNull(objectFragment);
        assertNotNull(objectFragment.fragment());
        assertTrue(objectFragment.fragment() instanceof JsonObject);

        assertNotNull(intFragment);
        assertNotNull(intFragment.fragment());
        assertTrue(intFragment.fragment() instanceof Integer);

        assertNotNull(stringFragment);
        assertNotNull(stringFragment.fragment());
        assertTrue(stringFragment.fragment() instanceof String);

        assertNotNull(arrayFragment);
        assertNotNull(arrayFragment.fragment());
        assertTrue(arrayFragment.fragment() instanceof JsonArray);

        assertNotNull(booleanFragment);
        assertNotNull(booleanFragment.fragment());
        assertTrue(booleanFragment.fragment() instanceof Boolean);
    }

    @Test
    public void testGetInWitTargetClass() {
        DocumentFragment<JsonObject> fragment = ctx.bucket().getIn(key, "sub", JsonObject.class);

        assertNotNull(fragment);
        assertNotNull(fragment.fragment());
        assertEquals("original", fragment.fragment().get("value"));
    }

    @Test(expected = DocumentDoesNotExistException.class)
    public void testGetInOnUnknownDocumentThrowsException() {
        ctx.bucket().getIn("blabla", "array", Object.class);
    }

    @Test
    public void testGetInUnknownPathReturnsNull() {
        DocumentFragment<Object> fragment = ctx.bucket().getIn(key, "badPath", Object.class);
        assertNull(fragment);
    }

    @Test
    public void testExistsIn() {
        assertTrue(ctx.bucket().existsIn(key, "sub"));
        assertTrue(ctx.bucket().existsIn(key, "int"));
        assertTrue(ctx.bucket().existsIn(key, "string"));
        assertTrue(ctx.bucket().existsIn(key, "array"));
        assertTrue(ctx.bucket().existsIn(key, "boolean"));
        assertFalse(ctx.bucket().existsIn(key, "somePathBlaBla"));
    }


    @Test(expected = DocumentDoesNotExistException.class)
    public void testExistsInOnUnknownDocumentThrowsException() {
        ctx.bucket().existsIn("blabla", "array");
    }

    @Test
    public void testExistsInUnknownPathReturnsFalse() {
        boolean exist = ctx.bucket().existsIn(key, "badPath");
        assertFalse(exist);
    }

    //=== UPSERT ===
    @Test
    public void testUpsertInDictionaryCreates() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub.newValue", "sValue");
        DocumentFragment result = ctx.bucket().upsertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        assertEquals("sValue", ctx.bucket().get(key).content().getObject("sub").getString("newValue"));
    }

    @Test
    public void testUpsertInDictionaryUpdates() {
        DocumentFragment<Boolean> fragment = new DocumentFragment<Boolean>(key, "sub.value", true);
        DocumentFragment result = ctx.bucket().upsertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        assertEquals(Boolean.TRUE, ctx.bucket().get(key).content().getObject("sub").getBoolean("value"));
    }

    @Test(expected = PathNotFoundException.class)
    public void testUpsertInDictionaryExtraLevelFails() {
        DocumentFragment<Integer> fragment = new DocumentFragment<Integer>(key, "sub.some.path", 1024);
        ctx.bucket().upsertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test
    public void testUpsertInDictionaryExtraLevelSucceedsWithCreatesParents() {
        DocumentFragment<Integer> fragment = new DocumentFragment<Integer>(key, "sub.some.path", 1024);
        DocumentFragment result = ctx.bucket().upsertIn(fragment, true, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        int content = ctx.bucket().get(key).content().getObject("sub").getObject("some").getInt("path");
        assertEquals(1024, content);
    }

    @Test(expected = PathMismatchException.class)
    public void testUpsertInScalarFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "boolean.some", "string");
        ctx.bucket().upsertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = PathMismatchException.class)
    public void testUpsertInArrayFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array.some", "string");
        ctx.bucket().upsertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = PathInvalidException.class)
    public void testUpsertInArrayIndexFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[1]", "string");
        ctx.bucket().upsertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = CASMismatchException.class)
    public void testUpsertInWithBadCas() {
        DocumentFragment<Long> fragment = new DocumentFragment<Long>(key, "int", null, 0, 1234L, null);
        ctx.bucket().upsertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    //=== INSERT ===
    @Test
    public void testInsertInDictionaryCreates() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub.newValue", "sValue");
        DocumentFragment result = ctx.bucket().insertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        assertEquals("sValue", ctx.bucket().get(key).content().getObject("sub").getString("newValue"));
    }

    @Test(expected = PathExistsException.class)
    public void testInsertInDictionaryDoesntUpdate() {
        DocumentFragment<Boolean> fragment = new DocumentFragment<Boolean>(key, "sub.value", true);
        ctx.bucket().insertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = PathNotFoundException.class)
    public void testInsertInDictionaryExtraLevelFails() {
        DocumentFragment<Integer> fragment = new DocumentFragment<Integer>(key, "sub.some.path", 1024);
        ctx.bucket().insertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test
    public void testInsertInDictionaryExtraLevelSucceedsWithCreatesParents() {
        DocumentFragment<Integer> fragment = new DocumentFragment<Integer>(key, "sub.some.path", 1024);
        DocumentFragment result = ctx.bucket().insertIn(fragment, true, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        int content = ctx.bucket().get(key).content().getObject("sub").getObject("some").getInt("path");
        assertEquals(1024, content);
    }

    @Test(expected = PathMismatchException.class)
    public void testInsertInScalarFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "boolean.some", "string");
        ctx.bucket().insertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = PathMismatchException.class)
    public void testInsertInArrayFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array.some", "string");
        ctx.bucket().insertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = PathInvalidException.class)
    public void testInsertInArrayIndexFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[1]", "string");
        ctx.bucket().insertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = CASMismatchException.class)
    public void testInsertInWithBadCas() {
        DocumentFragment<Long> fragment = new DocumentFragment<Long>(key, "int", null, 0, 1234L, null);
        ctx.bucket().insertIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    //=== REPLACE ===
    @Test(expected = PathNotFoundException.class)
    public void testReplaceInDictionaryDoesntCreate() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub.newValue", "sValue");
        ctx.bucket().replaceIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test
    public void testReplaceInDictionaryUpdates() {
        DocumentFragment<Boolean> fragment = new DocumentFragment<Boolean>(key, "sub.value", true);
        DocumentFragment result = ctx.bucket().replaceIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        assertEquals(Boolean.TRUE, ctx.bucket().get(key).content().getObject("sub").getBoolean("value"));
    }
    
    @Test(expected = PathMismatchException.class)
    public void testReplaceInScalarFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "boolean.some", "string");
        ctx.bucket().replaceIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = PathMismatchException.class)
    public void testReplaceInArrayFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array.some", "string");
        ctx.bucket().replaceIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test
    public void testReplaceInArrayIndexUpdates() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[1]", "string");
        DocumentFragment<String> result = ctx.bucket().replaceIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        assertEquals("string", ctx.bucket().get(key).content().getArray("array").getString(1));
    }

    @Test(expected = PathNotFoundException.class)
    public void testReplaceInArrayIndexOutOfBoundsFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[3]", "badIndex");
        ctx.bucket().replaceIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = CASMismatchException.class)
    public void testReplaceInWithBadCas() {
        DocumentFragment<Long> fragment = new DocumentFragment<Long>(key, "int", null, 0, 1234L, null);
        ctx.bucket().replaceIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    //=== EXTEND ===
    @Test(expected = PathMismatchException.class)
    public void testExtendOnNonArrayFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub", "string");
        ctx.bucket().extendIn(fragment, ExtendDirection.BACK, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test
    public void testExtendAtBackOfArray() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array", "newElement");
        DocumentFragment<String> result = ctx.bucket().extendIn(fragment, ExtendDirection.BACK, false, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray array = ctx.bucket().get(key).content().getArray("array");
        assertEquals(4, array.size());
        assertEquals("newElement", array.getString(3));
    }

    @Test
    public void testExtendAtFrontOfArray() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array", "newElement");
        DocumentFragment<String> result = ctx.bucket().extendIn(fragment, ExtendDirection.FRONT, false, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray array = ctx.bucket().get(key).content().getArray("array");
        assertEquals(4, array.size());
        assertEquals("newElement", array.getString(0));
    }

    @Test
    public void testExtendInDictionaryWithCreateParentsCreatesArray() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub.array", "newElement");
        DocumentFragment<String> result = ctx.bucket().extendIn(fragment, ExtendDirection.FRONT, true, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray array = ctx.bucket().get(key).content().getObject("sub").getArray("array");
        assertEquals(1, array.size());
        assertEquals("newElement", array.getString(0));
    }

    @Test(expected = PathNotFoundException.class)
    public void testExtendInDictionnaryWithoutCreateParentsFails() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub.array", "newElement");
        ctx.bucket().extendIn(fragment, ExtendDirection.FRONT, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    //TODO as of 12/1/2016 the spec says this should work but server returns PathInvalid
    @Test
    public void testExtendAtBackOfRootArrayWorks() {
        ctx.bucket().upsert(JsonArrayDocument.create("subdocArray", JsonArray.empty()));

        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "", "unique");
        DocumentFragment<String> result = ctx.bucket().extendIn(fragment, ExtendDirection.BACK, false, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray array = ctx.bucket().get("subdocArray", JsonArrayDocument.class).content();
        assertEquals(1, array.size());
        assertEquals("unique", array.getString(0));

        DocumentFragment<String> fragment2 = new DocumentFragment<String>(key, "", "back");
        DocumentFragment<String> result2 = ctx.bucket().extendIn(fragment2, ExtendDirection.BACK, false, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result2);
        assertNotEquals(fragment.cas(), result2.cas());
        array = ctx.bucket().get("subdocArray", JsonArrayDocument.class).content();
        assertEquals(2, array.size());
        assertEquals("unique", array.getString(0));
        assertEquals("back", array.getString(1));
    }

    //TODO as of 12/1/2016 the spec says this should work but server returns PathInvalid
    @Test
    public void testExtendAtFrontOfRootArrayWorks() {
        ctx.bucket().upsert(JsonArrayDocument.create("subdocArray", JsonArray.empty()));

        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "", "unique");
        DocumentFragment<String> result = ctx.bucket().extendIn(fragment, ExtendDirection.FRONT, true, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray array = ctx.bucket().get("subdocArray", JsonArrayDocument.class).content();
        assertEquals(1, array.size());
        assertEquals("unique", array.getString(0));

        DocumentFragment<String> fragment2 = new DocumentFragment<String>(key, "", "front");
        DocumentFragment<String> result2 = ctx.bucket().extendIn(fragment2, ExtendDirection.FRONT, true, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result2);
        assertNotEquals(fragment.cas(), result2.cas());
        array = ctx.bucket().get("subdocArray", JsonArrayDocument.class).content();
        assertEquals(2, array.size());
        assertEquals("front", array.getString(0));
        assertEquals("unique", array.getString(1));
    }

    @Test(expected = CASMismatchException.class)
    public void testExtendInWithBadCas() {
        DocumentFragment<Long> fragment = new DocumentFragment<Long>(key, "int", null, 0, 1234L, null);
        ctx.bucket().extendIn(fragment, ExtendDirection.BACK, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    //=== ARRAY INSERT ===
    @Test
    public void testArrayInsertAtIndexZero() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[0]", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(4, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(0));
    }
    
    @Test
    public void testArrayInsertAtSize() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[3]", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(4, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(3));
    }
    
    @Test
    public void testArrayInsertAtIndexZeroOnEmptyArray() {
        //prepare doc with empty array
        JsonObject withEmptyArray = JsonObject.create().put("array", JsonArray.empty());
        ctx.bucket().upsert(JsonDocument.create(key, withEmptyArray));

        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[0]", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(1, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(0));
    }
    
    @Test
    public void testArrayInsertAtExistingIndex() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[1]", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(4, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(1));
        assertEquals(2, storedArray.getInt(2).intValue());
        assertEquals(true, storedArray.getBoolean(3));
    }
    
    @Test(expected = PathNotFoundException.class)
    public void testArrayInsertAtIndexOutOfBounds() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[5]", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    //TODO as of 12/1/2016 the spec says PathNotFound but server returns PathInvalid (PathInvalid preferred)
    @Test(expected = PathNotFoundException.class)
    public void testArrayInsertAtNegativeIndex() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[-1]", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = PathNotFoundException.class)
    public void testArrayInsertOnArrayThatDoesntExist() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "secondArray[0]", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    //TODO as of 12/1/2016 the spec says PathMismatch but server returns PathInvalid (spec preferred)
    @Test(expected = PathMismatchException.class)
    public void testArrayInsertOnPathNotEndingWithArrayElement() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testArrayInsertOnEmptyPath() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = CASMismatchException.class)
    public void testArrayInsertWithBadCas() {
        DocumentFragment<Long> fragment = new DocumentFragment<Long>(key, "int", null, 0, 1234L, null);
        ctx.bucket().arrayInsertIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    //=== ARRAY ADD UNIQUE ===
    @Test(expected = PathMismatchException.class)
    public void testArrayAddUniqueInNonArray() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().addUniqueIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(1, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(0));
    }
    
    @Test(expected = PathMismatchException.class)
    public void testArrayAddUniqueInArrayWithNonPrimitives() {
        //create document with array containing array
        JsonObject root = JsonObject.create().put("array", JsonArray.create().add(JsonArray.empty()));
        ctx.bucket().upsert(JsonDocument.create(key, root));

        //not a primitive only array => MISMATCH
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().addUniqueIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }
    
    @Test(expected = CannotInsertValueException.class)
    public void testArrayAddUniqueWithNonPrimitiveFragment() {
        DocumentFragment<JsonObject> fragment = new DocumentFragment<JsonObject>(key, "array", JsonObject.create().put("object", true));
        DocumentFragment<JsonObject> result = ctx.bucket().addUniqueIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
        System.out.println(ctx.bucket().get(key));
    }
    
    @Test(expected = PathExistsException.class)
    public void testArrayAddUniqueWithValueAlreadyPresent() {
        DocumentFragment<Boolean> fragment = new DocumentFragment<Boolean>(key, "array", true);
        DocumentFragment<Boolean> result = ctx.bucket().addUniqueIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }
    
    @Test(expected = PathNotFoundException.class)
    public void testArrayAddUniqueOnNonExistingArray() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "anotherArray", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().addUniqueIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test
    public void testArrayAddUniqueOnNonExistingArraySucceedsWithCreateParents() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "anotherArray", "arrayInsert");
        DocumentFragment<String> result = ctx.bucket().addUniqueIn(fragment, true, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("anotherArray");
        assertEquals(1, storedArray.size());
        assertEquals("arrayInsert", storedArray.getString(0));
    }

    @Test(expected = CASMismatchException.class)
    public void testArrayAddUniqueWithBadCas() {
        DocumentFragment<Long> fragment = new DocumentFragment<Long>(key, "int", null, 0, 1234L, null);
        ctx.bucket().addUniqueIn(fragment, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    //=== REMOVE ===
    @Test
    public void testRemoveScalar() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "int", null);
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNull(result.fragment());
        assertNotEquals(fragment.cas(), result.cas());
        assertFalse(ctx.bucket().get(key).content().containsKey("int"));
    }

    @Test
    public void testRemoveIgnoreFragment() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "int", "anyFragmentGoesThere");
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNull(result.fragment());
        assertFalse(ctx.bucket().get(key).content().toString().contains("anyFragmentGoesThere"));
    }

    @Test
    public void testRemoveDictEntry() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub.value", null);
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNull(result.fragment());
        assertNotEquals(fragment.cas(), result.cas());
        assertEquals(0, ctx.bucket().get(key).content().getObject("sub").size());
    }
    
    @Test
    public void testRemoveArrayElement() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[1]", null);
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNull(result.fragment());
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(2, storedArray.size());
        assertEquals("1", storedArray.getString(0));
        assertEquals(true, storedArray.getBoolean(1));
    }

    @Test
    public void testRemoveLastItem() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[-1]", null);
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNull(result.fragment());
        assertNotEquals(fragment.cas(), result.cas());
        JsonArray storedArray = ctx.bucket().get(key).content().getArray("array");
        assertEquals(2, storedArray.size());
        assertEquals("1", storedArray.getString(0));
        assertEquals(2, storedArray.getInt(1).intValue());
    }
    
    @Test(expected = PathNotFoundException.class)
    public void testRemoveScalarWithBadPath() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "integer", null);
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);

        assertNotNull(result);
        assertNull(result.fragment());
        assertNotEquals(fragment.cas(), result.cas());
        assertFalse(ctx.bucket().get(key).content().containsKey("int"));
    }
    
    @Test(expected = PathNotFoundException.class)
    public void testRemoveDictEntryWithBadKey() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "sub.valuezz", null);
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }
    
    @Test(expected = PathNotFoundException.class)
    public void testRemoveArrayElementWithIndexOutOfBounds() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "array[4]", null);
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveEmptyPath() {
        DocumentFragment<String> fragment = new DocumentFragment<String>(key, "", null);
        DocumentFragment<String> result = ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = CASMismatchException.class)
    public void testRemoveWithBadCas() {
        DocumentFragment<Long> fragment = new DocumentFragment<Long>(key, "int", null, 0, 1234L, null);
        ctx.bucket().removeIn(fragment, PersistTo.NONE, ReplicateTo.NONE);
    }

    //=== COUNTER ===
    @Test
    public void testCounterWithPositiveDeltaIncrements() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "int", 1000L);
        DocumentFragment<Long> result = ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);

        assertEquals(1123L, result.fragment().longValue());
        assertEquals(1123L, ctx.bucket().get(key).content().getLong("int").longValue());
    }

    @Test
    public void testCounterWithNegativeDeltaDecrements() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "int", -123L);
        DocumentFragment<Long> result = ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);

        assertEquals(0L, result.fragment().longValue());
        assertEquals(0L, ctx.bucket().get(key).content().getLong("int").longValue());
    }

    @Test(expected = ZeroDeltaException.class)
    public void testCounterWithZeroDeltaFails() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "int", 0L);
        ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    //TODO is there a way of testing for NumberTooBigException (the stored number would have to be greater than Long.MAX.VALUE)

    @Test
    public void testCounterProducingTooLargeValueFails() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "int", Long.MAX_VALUE - 123L);

        //first increment should work
        DocumentFragment<Long> result = ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);
        assertNotNull(result);
        assertEquals(Long.MAX_VALUE, result.fragment().longValue());

        //second increment should fail
        try {
            result = ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);
            fail("second counter increment should have made the counter value too big");
        } catch (DeltaTooBigException e) {
            //success
        }
    }

    @Test
    public void testCounterInPartialPathMissingLastPathElementCreatesNewCounter() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "sub.counter", 1000L);
        DocumentFragment<Long> result = ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);

        assertEquals(1000L, result.fragment().longValue());
        assertEquals(1000L, ctx.bucket().get(key).content().getObject("sub").getLong("counter").longValue());
    }

    @Test
    public void testCounterDeltaUpperBoundIsLongMaxValue() {
        long expected = Long.MAX_VALUE;
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "newCounter", expected);
        DocumentFragment<Long> result = ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);

        assertEquals(expected, result.fragment().longValue());
        assertEquals(expected, ctx.bucket().get(key).content().getLong("newCounter").longValue());
    }

    @Test
    public void testCounterWithLongMinValueDeltaSucceedsOnNewCounter() {
        long expected = Long.MIN_VALUE + 1L;
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "newCounter", expected);
        DocumentFragment<Long> result = ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);

        assertEquals(expected, result.fragment().longValue());
        assertEquals(expected, ctx.bucket().get(key).content().getLong("newCounter").longValue());
    }

    @Test(expected = PathMismatchException.class)
    public void testCounterOnNonNumericPathFails() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "sub.value", 1000L);
        ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = PathNotFoundException.class)
    public void testCounterInPartialPathMissingIntermediaryElementFails() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "counters.a", 1000L);
        ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test
    public void testCounterInPartialPathMissingIntermediaryElementWithCreateParentsSucceeds() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "counters.a", 1000L);
        DocumentFragment<Long> result = ctx.bucket().counterIn(delta, true, PersistTo.NONE, ReplicateTo.NONE);

        assertEquals(1000L, result.fragment().longValue());
        assertEquals(1000L, ctx.bucket().get(key).content().getObject("counters").getLong("a").longValue());
    }

    @Test(expected = CASMismatchException.class)
    public void testCounterInWithBadCas() {
        DocumentFragment<Long> delta = new DocumentFragment<Long>(key, "int", 1000L, 0, 1234L, null);
        ctx.bucket().counterIn(delta, false, PersistTo.NONE, ReplicateTo.NONE);
    }

    //=== MULTI LOOKUP ===

    @Test(expected = IllegalArgumentException.class)
    public void testMultiLookupEmptySpecFails() {
        ctx.bucket().lookupIn(key);
    }

    @Test(expected = NullPointerException.class)
    public void testMultiLookupNullSpecFails() {
        ctx.bucket().lookupIn(key, null);
    }

    @Test
    public void testMultiLookup() {
        DocumentFragment<List<LookupResult>> resultPayload = ctx.bucket().lookupIn(key, LookupSpec.get("boolean"),
                LookupSpec.get("sub"), LookupSpec.exists("string"), LookupSpec.exists("no"));

        assertNotNull(resultPayload);
        List<LookupResult> results = resultPayload.fragment();
        assertEquals(4, results.size());
        assertEquals("boolean", results.get(0).path());
        assertEquals("sub", results.get(1).path());
        assertEquals("string", results.get(2).path());
        assertEquals("no", results.get(3).path());
        assertTrue(results.get(0).value() instanceof Boolean);
        assertTrue(results.get(1).value() instanceof JsonObject);
        assertTrue(results.get(2).value() instanceof Boolean);
        assertTrue(results.get(3).value() instanceof Boolean);
        assertEquals(true, results.get(2).value());
        assertEquals(false, results.get(3).value());
    }

    @Test
    public void testMultiLookupExistDoesNotFailOnBadPath() {
        DocumentFragment<List<LookupResult>> resultPayload = ctx.bucket().lookupIn(key, LookupSpec.exists("sub[1]"));
        assertNotNull(resultPayload);
        List<LookupResult> results = resultPayload.fragment();
        assertEquals(1, results.size());
        LookupResult result = results.get(0);
        assertTrue(result.isSuccess());
        assertEquals(false, result.value());
    }

    @Test
    public void testMultiLookupGetDoesNotFailOnBadPath() {
        DocumentFragment<List<LookupResult>> resultPayload = ctx.bucket()
                .lookupIn(key, LookupSpec.get("sub"), LookupSpec.get("sub[1]"), LookupSpec.get("badPath"));
        assertNotNull(resultPayload);
        List<LookupResult> results = resultPayload.fragment();
        assertEquals(3, results.size());

        LookupResult result = results.get(0);
        assertNotNull(result.value());
        assertTrue(result.isSuccess());
        assertEquals(ResponseStatus.SUCCESS, result.status());

        result = results.get(1);
        assertNull(result.value());
        assertFalse(result.isSuccess());
        assertEquals(ResponseStatus.SUBDOC_PATH_MISMATCH, result.status());

        result = results.get(2);
        assertNull(result.value());
        assertFalse(result.isSuccess());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, result.status());
    }

    //=== MULTI MUTATION ===
    @Test
    public void testMultiMutation() {
        JsonDocument doc = ctx.bucket().mutateIn(JsonDocument.create(key), PersistTo.NONE, ReplicateTo.NONE,
                MutationSpec.replace("sub.value", "replaced"),
                MutationSpec.replace("int", 1024),
                MutationSpec.upsert("sub.otherValue", "newValue", false));

        JsonDocument stored = ctx.bucket().get(key);

        assertNotNull(doc);
        assertEquals(stored, doc);
        assertNotEquals(testJson, doc.content());
        assertEquals(doc.content(), stored.content());
        assertEquals("replaced", doc.content().getObject("sub").getString("value"));
        assertEquals("newValue", doc.content().getObject("sub").getString("otherValue"));
        assertEquals(1024, doc.content().getInt("int").intValue());
    }

    @Test
    public void testMultiMutationWithFailure() {
        try {
            ctx.bucket().mutateIn(JsonDocument.create(key), PersistTo.NONE, ReplicateTo.NONE,
                    MutationSpec.replace("sub.value", "replaced"),
                    MutationSpec.replace("int", 1024),
                    MutationSpec.upsert("sub.otherValue.deeper", "newValue", false),
                    MutationSpec.replace("secondError", "unreachable"));
            fail("Expected MultiMutationException");
        } catch (MultiMutationException e) {
            assertEquals(2, e.firstFailureIndex());
            assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, e.firstFailureStatus());
            assertNotNull(e.getCause());
            assertTrue(e.getCause().toString(), e.getCause() instanceof  PathNotFoundException);
            assertTrue(e.getCause().toString(), e.getCause().toString().contains("sub.otherValue.deeper"));
            assertEquals(4, e.originalSpec().size());
        }

        assertEquals(testJson, ctx.bucket().get(key).content());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultiMutationWithEmptySpecFails() {
        ctx.bucket().mutateIn(JsonDocument.create(key), PersistTo.NONE, ReplicateTo.NONE);
    }

    @Test(expected = NullPointerException.class)
    public void testMultiMutationWithNullSpecFails() {
        ctx.bucket().mutateIn(JsonDocument.create(key), PersistTo.NONE, ReplicateTo.NONE, (MutationSpec[]) null);
    }

    @Test(expected = CASMismatchException.class)
    public void testMultiMutationWithBadCas() {
        ctx.bucket().mutateIn(JsonDocument.create(key, null, 1234L), PersistTo.NONE, ReplicateTo.NONE, MutationSpec.replace("sub", 123));
    }
}