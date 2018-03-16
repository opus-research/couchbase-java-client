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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.document.subdoc.DocumentFragment;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.subdoc.LookupSpec;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.util.CouchbaseTestContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the sub-document API in {@link Bucket}.
 */
public class SubDocumentTest {

    private static CouchbaseTestContext ctx;
    private static String key = "SubdocAPI";
    private static JsonObject testJson;

    @BeforeClass
    public static void connect() throws Exception {
        ctx = CouchbaseTestContext.builder()
                .bucketQuota(256)
                .bucketType(BucketType.COUCHBASE)
                .flushOnInit(true)
                .build();

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
        List<DocumentFragment<Object>> results = ctx.bucket().lookupIn(key, LookupSpec.get("boolean"),
                LookupSpec.get("sub"), LookupSpec.exists("string"), LookupSpec.exists("no"));

        assertNotNull(results);
        assertEquals(4, results.size());
        assertEquals("boolean", results.get(0).path());
        assertEquals("sub", results.get(1).path());
        assertEquals("string", results.get(2).path());
        assertEquals("no", results.get(3).path());
        assertTrue(results.get(0).fragment() instanceof Boolean);
        assertTrue(results.get(1).fragment() instanceof JsonObject);
        assertTrue(results.get(2).fragment() instanceof Boolean);
        assertTrue(results.get(3).fragment() instanceof Boolean);
        assertEquals(true, results.get(2).fragment());
        assertEquals(false, results.get(3).fragment());
    }

    @Test
    public void testMultiLookupExistDoesNotFailOnBadPath() {
        List<DocumentFragment<Object>> results = ctx.bucket().lookupIn(key, LookupSpec.exists("sub[1]"));
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(false, results.get(0).fragment());
    }

    @Test
    public void testMultiLookupGetDoesNotFailOnBadPath() {
        List<DocumentFragment<Object>> results = ctx.bucket().lookupIn(key, LookupSpec.get("sub[1]"));
        assertNotNull(results);
        assertEquals(1, results.size());
        assertNull(results.get(0).fragment());
    }
}
