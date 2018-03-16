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
package com.couchbase.client.java.document.json;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Verifies the functionality provided by a {@link JsonObject}.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 2.0
 */
public class JsonObjectTest {

    @Test
    public void shouldExportEmptyObject() {
        String result = JsonObject.empty().toString();
        assertEquals("{}", result);
    }

    @Test
    public void shouldExportStrings() {
        String result = JsonObject.empty().put("key", "value").toString();
        assertEquals("{\"key\":\"value\"}", result);
    }

    @Test
    public void shouldExportNestedObjects() {
        JsonObject obj = JsonObject.empty()
            .put("nested", JsonObject.empty().put("a", true));
        assertEquals("{\"nested\":{\"a\":true}}", obj.toString());
    }

    @Test
    public void shouldExportNestedArrays() {
        JsonObject obj = JsonObject.empty()
            .put("nested", JsonArray.empty().add(true).add(4).add("foo"));
        assertEquals("{\"nested\":[true,4,\"foo\"]}", obj.toString());
    }

    @Test
    public void shouldReturnNullWhenNotFound() {
        JsonObject obj = JsonObject.empty();
        assertNull(obj.getInt("notfound"));
    }

    @Test
    public void shouldEqualBasedOnItsProperties() {
        JsonObject obj1 = JsonObject.create().put("foo", "bar");
        JsonObject obj2 = JsonObject.create().put("foo", "bar");
        assertEquals(obj1, obj2);

        obj1 = JsonObject.create().put("foo", "baz");
        obj2 = JsonObject.create().put("foo", "bar");
        assertNotEquals(obj1, obj2);

        obj1 = JsonObject.create().put("foo", "bar").put("bar", "baz");
        obj2 = JsonObject.create().put("foo", "bar");
        assertNotEquals(obj1, obj2);
    }

    @Test
    public void shouldConvertNumbers() {
        JsonObject obj = JsonObject.create().put("number", 1L);

        assertEquals(new Double(1.0d), obj.getDouble("number"));
        assertEquals(new Long(1L), obj.getLong("number"));
        assertEquals(new Integer(1), obj.getInt("number"));
    }

    @Test
    public void shouldConvertOverflowNumbers() {
        int maxValue = Integer.MAX_VALUE; //int max value is 2147483647
        long largeValue = maxValue + 3L;
        double largerThanIntMaxValue = largeValue + 0.56d;

        JsonObject obj = JsonObject.create().put("number", largerThanIntMaxValue);
        assertEquals(new Double(largerThanIntMaxValue), obj.getDouble("number"));
        assertEquals(new Long(largeValue), obj.getLong("number"));
        assertEquals(new Integer(maxValue), obj.getInt("number"));
    }

    @Test
    public void shouldNotNullPointerOnGetNumber() {
        JsonObject obj = JsonObject.empty();

        assertNull(obj.getDouble("number"));
        assertNull(obj.getLong("number"));
        assertNull(obj.getInt("number"));
    }

    @Test
    public void shouldConstructEmptyFromEmptyMapOrNull() {
        JsonObject obj = JsonObject.from(Collections.<String, Object>emptyMap());
        assertNotNull(obj);
        assertTrue(obj.isEmpty());

        obj = JsonObject.from(null);
        assertNotNull(obj);
        assertTrue(obj.isEmpty());
    }

    @Test
    public void shouldConstructJsonObjectFromMap() {
        String item1 = "item1";
        Double item2 = 2.2d;
        Long item3 = 3L;
        Boolean item4 = true;
        JsonArray item5 = JsonArray.empty();
        JsonObject item6 = JsonObject.empty();
        Map<String, Object> source = new HashMap<String, Object>(6);
        source.put("key1", item1);
        source.put("key2", item2);
        source.put("key3", item3);
        source.put("key4", item4);
        source.put("key5", item5);
        source.put("key6", item6);

        JsonObject obj = JsonObject.from(source);
        assertNotNull(obj);
        assertEquals(6, obj.size());
        assertEquals(item1, obj.get("key1"));
        assertEquals(item2, obj.get("key2"));
        assertEquals(item3, obj.get("key3"));
        assertEquals(item4, obj.get("key4"));
        assertEquals(item5, obj.get("key5"));
        assertEquals(item6, obj.get("key6"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldDetectNullKeyInMap() {
        Map<String, Double> badMap = new HashMap<String, Double>(2);
        badMap.put("key1", 1.1d);
        badMap.put(null, 2.2d);
        JsonObject.from(badMap);
    }

    @Test
    public void shouldDetectNullValueInMap() {
        Map<String, Long> badMap = new HashMap<String, Long>(2);
        badMap.put("key1", 1L);
        badMap.put("key2", null);

        try {
            JsonObject obj = JsonObject.from(badMap);
        } catch (NullPointerException e) {
            if (!e.getMessage().contains("key2")) {
                fail("Null value should output incriminating key");
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDetectIncorrectItemInMap() {
        Object badItem = new java.lang.CloneNotSupportedException();
        Map<String, Object> badMap = new HashMap<String, Object>(1);
        badMap.put("key1", badItem);
        JsonObject.from(badMap);
    }

}