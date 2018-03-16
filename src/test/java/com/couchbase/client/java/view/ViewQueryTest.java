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
package com.couchbase.client.java.view;

import com.couchbase.client.java.SerializationHelper;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Verifies the correct functionality of the {@link ViewQuery} DSL.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public class ViewQueryTest {

    @Test
    public void shouldSetDefaults() {
        ViewQuery query = ViewQuery.from("design", "view");
        assertEquals("design", query.getDesign());
        assertEquals("view", query.getView());
        assertFalse(query.isDevelopment());
        assertEquals("", query.toQueryString());
        assertEquals("ViewQuery(design/view){params=\"\"}", query.toString());
    }

    @Test
    public void shouldReduce() {
        ViewQuery query = ViewQuery.from("design", "view").reduce();
        assertEquals("reduce=true", query.toQueryString());

        query = ViewQuery.from("design", "view").reduce(true);
        assertEquals("reduce=true", query.toQueryString());

        query = ViewQuery.from("design", "view").reduce(false);
        assertEquals("reduce=false", query.toQueryString());
    }

    @Test
    public void shouldLimit() {
        ViewQuery query = ViewQuery.from("design", "view").limit(10);
        assertEquals("limit=10", query.toQueryString());
    }

    @Test
    public void shouldSkip() {
        ViewQuery query = ViewQuery.from("design", "view").skip(3);
        assertEquals("skip=3", query.toQueryString());
    }

    @Test
    public void shouldGroup() {
        ViewQuery query = ViewQuery.from("design", "view").group();
        assertEquals("group=true", query.toQueryString());

        query = ViewQuery.from("design", "view").group(false);
        assertEquals("group=false", query.toQueryString());
    }

    @Test
    public void shouldGroupLevel() {
        ViewQuery query = ViewQuery.from("design", "view").groupLevel(2);
        assertEquals("group_level=2", query.toQueryString());
    }

    @Test
    public void shouldSetInclusiveEnd() {
        ViewQuery query = ViewQuery.from("design", "view").inclusiveEnd();
        assertEquals("inclusive_end=true", query.toQueryString());

        query = ViewQuery.from("design", "view").inclusiveEnd(false);
        assertEquals("inclusive_end=false", query.toQueryString());
    }

    @Test
    public void shouldSetStale() {
        ViewQuery query = ViewQuery.from("design", "view").stale(Stale.FALSE);
        assertEquals("stale=false", query.toQueryString());

        query = ViewQuery.from("design", "view").stale(Stale.TRUE);
        assertEquals("stale=ok", query.toQueryString());

        query = ViewQuery.from("design", "view").stale(Stale.UPDATE_AFTER);
        assertEquals("stale=update_after", query.toQueryString());
    }

    @Test
    public void shouldSetOnError() {
        ViewQuery query = ViewQuery.from("design", "view").onError(OnError.CONTINUE);
        assertEquals("on_error=continue", query.toQueryString());

        query = ViewQuery.from("design", "view").onError(OnError.STOP);
        assertEquals("on_error=stop", query.toQueryString());

    }

    @Test
    public void shouldSetDebug() {
        ViewQuery query = ViewQuery.from("design", "view").debug();
        assertEquals("debug=true", query.toQueryString());

        query = ViewQuery.from("design", "view").debug(false);
        assertEquals("debug=false", query.toQueryString());
    }

    @Test
    public void shouldSetDescending() {
        ViewQuery query = ViewQuery.from("design", "view").descending();
        assertEquals("descending=true", query.toQueryString());

        query = ViewQuery.from("design", "view").descending(false);
        assertEquals("descending=false", query.toQueryString());
    }

    @Test
    public void shouldHandleKey() {
        ViewQuery query = ViewQuery.from("design", "view").key("key");
        assertEquals("key=%22key%22", query.toQueryString());

        query = ViewQuery.from("design", "view").key(1);
        assertEquals("key=1", query.toQueryString());

        query = ViewQuery.from("design", "view").key(true);
        assertEquals("key=true", query.toQueryString());

        query = ViewQuery.from("design", "view").key(3.55);
        assertEquals("key=3.55", query.toQueryString());

        query = ViewQuery.from("design", "view").key(JsonArray.from("foo", 3));
        assertEquals("key=%5B%22foo%22%2C3%5D", query.toQueryString());

        query = ViewQuery.from("design", "view").key(JsonObject.empty().put("foo", true));
        assertEquals("key=%7B%22foo%22%3Atrue%7D", query.toQueryString());
    }

    @Test
    public void shouldHandleKeys() {
        JsonArray keysArray = JsonArray.from("foo", 3, true);
        ViewQuery query = ViewQuery.from("design", "view").keys(keysArray);
        assertEquals("", query.toQueryString());
        assertEquals(keysArray.toString(), query.getKeys());
    }

    @Test
    public void shouldOutputSmallKeysInToString() {
        JsonArray keysArray = JsonArray.from("foo", 3, true);
        ViewQuery query = ViewQuery.from("design", "view").keys(keysArray);
        assertEquals("", query.toQueryString());
        assertEquals("ViewQuery(design/view){params=\"\", keys=\"[\"foo\",3,true]\"}", query.toString());
    }

    @Test
    public void shouldTruncateLargeKeysInToString() {
        StringBuilder largeString = new StringBuilder(142);
        for (int i = 0; i < 140; i++) {
            largeString.append('a');
        }
        largeString.append("bc");
        JsonArray keysArray = JsonArray.from(largeString.toString());
        ViewQuery query = ViewQuery.from("design", "view").keys(keysArray);
        assertEquals("", query.toQueryString());
        assertEquals("ViewQuery(design/view){params=\"\", keys=\"[\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaa...\"(146 chars total)}", query.toString());
    }

    @Test
    public void shouldOutputDesignDocViewDevAndIncludeDocsInToString() {
        ViewQuery query = ViewQuery.from("a", "b").includeDocs().development();
        assertEquals("", query.toQueryString());
        assertEquals("ViewQuery(a/b){params=\"\", dev, includeDocs}", query.toString());
    }

    @Test
    public void shouldHandleStartKey() {
        ViewQuery query = ViewQuery.from("design", "view").startKey("key");
        assertEquals("startkey=%22key%22", query.toQueryString());

        query = ViewQuery.from("design", "view").startKey(1);
        assertEquals("startkey=1", query.toQueryString());

        query = ViewQuery.from("design", "view").startKey(true);
        assertEquals("startkey=true", query.toQueryString());

        query = ViewQuery.from("design", "view").startKey(3.55);
        assertEquals("startkey=3.55", query.toQueryString());

        query = ViewQuery.from("design", "view").startKey(JsonArray.from("foo", 3));
        assertEquals("startkey=%5B%22foo%22%2C3%5D", query.toQueryString());

        query = ViewQuery.from("design", "view").startKey(JsonObject.empty().put("foo", true));
        assertEquals("startkey=%7B%22foo%22%3Atrue%7D", query.toQueryString());
    }

    @Test
    public void shouldHandleEndKey() {
        ViewQuery query = ViewQuery.from("design", "view").endKey("key");
        assertEquals("endkey=%22key%22", query.toQueryString());

        query = ViewQuery.from("design", "view").endKey(1);
        assertEquals("endkey=1", query.toQueryString());

        query = ViewQuery.from("design", "view").endKey(true);
        assertEquals("endkey=true", query.toQueryString());

        query = ViewQuery.from("design", "view").endKey(3.55);
        assertEquals("endkey=3.55", query.toQueryString());

        query = ViewQuery.from("design", "view").endKey(JsonArray.from("foo", 3));
        assertEquals("endkey=%5B%22foo%22%2C3%5D", query.toQueryString());

        query = ViewQuery.from("design", "view").endKey(JsonObject.empty().put("foo", true));
        assertEquals("endkey=%7B%22foo%22%3Atrue%7D", query.toQueryString());
    }

    @Test
    public void shouldHandleStartKeyDocID() {
        ViewQuery query = ViewQuery.from("design", "view").startKeyDocId("mykey");
        assertEquals("startkey_docid=mykey", query.toQueryString());
    }

    @Test
    public void shouldHandleEndKeyDocID() {
        ViewQuery query = ViewQuery.from("design", "view").endKeyDocId("mykey");
        assertEquals("endkey_docid=mykey", query.toQueryString());
    }

    @Test
    public void shouldRespectDevelopmentParam() {
        ViewQuery query = ViewQuery.from("design", "view").development(true);
        assertTrue(query.isDevelopment());

        query = ViewQuery.from("design", "view").development(false);
        assertFalse(query.isDevelopment());
    }

    @Test
    public void shouldConcatMoreParams() {
        ViewQuery query = ViewQuery.from("design", "view")
            .descending()
            .debug()
            .development()
            .group()
            .reduce(false)
            .startKey(JsonArray.from("foo", true));
        assertEquals("reduce=false&group=true&debug=true&descending=true&startkey=%5B%22foo%22%2Ctrue%5D",
                query.toQueryString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDisallowNegativeLimit() {
        ViewQuery.from("design", "view").limit(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDisallowNegativeSkip() {
        ViewQuery.from("design", "view").skip(-1);
    }

    @Test
    public void shouldToggleDevelopment() {
        ViewQuery query = ViewQuery.from("design", "view").development(true);
        assertTrue(query.isDevelopment());

        query = ViewQuery.from("design", "view").development(false);
        assertFalse(query.isDevelopment());
    }

    @Test
    public void shouldSupportSerialization() throws Exception {
        ViewQuery query = ViewQuery.from("design", "view")
            .descending()
            .debug()
            .development()
            .group()
            .reduce(false)
            .keys(JsonArray.from("1", "2"))
            .startKey(JsonArray.from("foo", true));

        byte[] serialized = SerializationHelper.serializeToBytes(query);
        assertNotNull(serialized);

        ViewQuery deserialized = SerializationHelper.deserializeFromBytes(serialized, ViewQuery.class);
        assertEquals(query, deserialized);
    }

    @Test
    public void shouldIncludeDocs() {
        ViewQuery query = ViewQuery.from("design", "view").includeDocs();
        assertTrue(query.isIncludeDocs());
        assertEquals(JsonDocument.class, query.includeDocsTarget());

        query = ViewQuery.from("design", "view").includeDocs(JsonDocument.class);
        assertTrue(query.isIncludeDocs());
        assertEquals(JsonDocument.class, query.includeDocsTarget());

        query = ViewQuery.from("design", "view");
        assertFalse(query.isIncludeDocs());
        assertNull(query.includeDocsTarget());

        query = ViewQuery.from("design", "view").includeDocs(false, RawJsonDocument.class);
        assertFalse(query.isIncludeDocs());
        assertEquals(RawJsonDocument.class, query.includeDocsTarget());
    }

    @Test
    public void shouldStoreKeysAsJsonOutsideParams() {
        JsonArray keys = JsonArray.create().add("1").add("2").add("3");
        String keysJson = keys.toString();
        ViewQuery query = ViewQuery.from("design", "view");
        assertNull(query.getKeys());

        query.keys(keys);
        assertEquals(keysJson, query.getKeys());
        assertFalse(query.toQueryString().contains("keys="));
        assertFalse(query.toQueryString().contains("3"));
    }
}
