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
package com.couchbase.client.java.convert;

import com.couchbase.client.core.message.document.CoreDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import io.netty.util.CharsetUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests which verify the functionality for the {@link JacksonJsonConverter}.
 */
public class JacksonJsonConverterTest {

  private JacksonJsonConverter converter;

  @Before
  public void setup() {
    converter = new JacksonJsonConverter();
  }

  @Test
  public void shouldEncodeEmptyJsonObject() {
    final JsonObject object = JsonObject.empty();
    final CoreDocument coreDocument = converter.encode(JsonDocument.create(null, object));

    assertEquals("{}", coreDocument.content().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeEmptyJsonObject() {
    final JsonObject object = converter.decode("{}");

    assertTrue(object.isEmpty());
  }

  @Test
  public void shouldEncodeEmptyJsonArray() {
    final JsonObject object = JsonObject.empty();
    object.put("array", JsonArray.empty());
    final CoreDocument coreDocument = converter.encode(JsonDocument.create(null, object));

    assertEquals("{\"array\":[]}", coreDocument.content().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeEmptyJsonArray() {
    final JsonObject object = converter.decode("{\"array\":[]}");

    assertFalse(object.isEmpty());
    assertTrue(object.getArray("array") != null);
    assertTrue(object.getArray("array").isEmpty());
  }

  @Test
  public void shouldEncodeSimpleJsonObject() {
    final JsonObject object = JsonObject.empty();
    object.put("string", "Hello World");
    object.put("integer", 1);
    object.put("long", Long.MAX_VALUE);
    object.put("double", 11.3322);
    object.put("boolean", true);

    final CoreDocument coreDocument = converter.encode(JsonDocument.create(null, object));
    final String expected = "{\"boolean\":true,\"string\":\"Hello World\",\"double\":11.3322,\"integer\":1," +
        "\"long\":9223372036854775807}";

    assertEquals(expected, coreDocument.content().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeSimpleJsonObject() {
    final String input = "{\"boolean\":true,\"string\":\"Hello World\",\"double\":11.3322,\"integer\":1," +
        "\"long\":9223372036854775807}";
    final JsonObject object = converter.decode(input);

    assertEquals(1, object.getInt("integer"));
    assertEquals("Hello World", object.getString("string"));
    assertEquals(Long.MAX_VALUE, object.getLong("long"));
    assertEquals(11.3322, object.getDouble("double"), 0);
    assertTrue(object.getBoolean("boolean"));
  }

  @Test
  public void shouldEncodeSimpleJsonArray() {
    final JsonArray array = JsonArray.empty();
    array.add("Hello World");
    array.add(1);
    array.add(Long.MAX_VALUE);
    array.add(11.3322);
    array.add(false);
    final JsonObject object = JsonObject.empty().put("array", array);
    final CoreDocument coreDocument = converter.encode(JsonDocument.create(null, object));
    final String expected = "{\"array\":[\"Hello World\",1,9223372036854775807,11.3322,false]}";

    assertEquals(expected, coreDocument.content().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeSimpleJsonArray() {
    final String input = "{\"array\":[\"Hello World\",1,9223372036854775807,11.3322,false]}";
    final JsonObject object = converter.decode(input);
    final JsonArray array = object.getArray("array");

    assertEquals("Hello World", array.getString(0));
    assertEquals(1, array.getInt(1));
    assertEquals(Long.MAX_VALUE, array.getLong(2));
    assertEquals(11.3322, array.getDouble(3), 0);
    assertFalse(array.getBoolean(4));
  }

  @Test
  public void shouldEncodeNestedJsonObjects() {
    final JsonObject inner = JsonObject.empty().put("foo", "bar");
    final JsonObject object = JsonObject.empty().put("object", JsonObject.empty().put("inner", inner));
    final CoreDocument coreDocument = converter.encode(JsonDocument.create(null, object));
    final String expected = "{\"object\":{\"inner\":{\"foo\":\"bar\"}}}";

    assertEquals(expected, coreDocument.content().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeNestedJsonObjects() {
    final String input = "{\"object\":{\"inner\":{\"foo\":\"bar\"}}}";
    final JsonObject object = converter.decode(input);

    assertEquals(1, object.size());
    assertEquals(1, object.getObject("object").size());
    assertEquals("bar", object.getObject("object").getObject("inner").get("foo"));
  }

  @Test
  public void shouldEncodeNestedJsonArrays() {
    final JsonObject object = JsonObject.empty().put("inner", JsonArray.empty().add(JsonArray.empty()));
    final CoreDocument coreDocument = converter.encode(JsonDocument.create(null, object));
    final String expected = "{\"inner\":[[]]}";

    assertEquals(expected, coreDocument.content().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeNestedJsonArray() {
    final String input = "{\"inner\":[[]]}";
    final JsonObject object = converter.decode(input);

    assertEquals(1, object.size());
    assertEquals(1, object.getArray("inner").size());
    assertTrue(object.getArray("inner").getArray(0).isEmpty());
  }

  @Test
  public void shouldEncodeMixedNestedJsonValues() {
    final JsonArray children = JsonArray.empty()
        .add(JsonObject.empty().put("name", "Jane Doe").put("age", 25))
        .add(JsonObject.empty().put("name", "Tom Doe").put("age", 13));
    final JsonObject object = JsonObject.empty()
        .put("firstname", "John")
        .put("lastname", "Doe")
        .put("colors", JsonArray.empty().add("red").add("blue"))
        .put("children", children)
        .put("active", true);
    final CoreDocument coreDocument = converter.encode(JsonDocument.create(null, object));
    final String expected = "{\"firstname\":\"John\"," +
        "\"children\":[{\"name\":\"Jane Doe\",\"age\":25},{\"name\":\"Tom Doe\",\"age\":13}]," +
        "\"active\":true,\"colors\":[\"red\",\"blue\"],\"lastname\":\"Doe\"}";

    assertEquals(expected, coreDocument.content().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeMixedNestedJsonValues() {
    final String input = "{\"firstname\":\"John\"," +
        "\"children\":[{\"name\":\"Jane Doe\",\"age\":25},{\"name\":\"Tom Doe\",\"age\":13}]," +
        "\"active\":true,\"colors\":[\"red\",\"blue\"],\"lastname\":\"Doe\"}";
    final JsonObject user = converter.decode(input);
    final JsonObject child0 = user.getArray("children").getObject(0);
    final JsonObject child1 = user.getArray("children").getObject(1);

    assertEquals("John", user.getString("firstname"));
    assertEquals("Doe", user.getString("lastname"));
    assertEquals(2, user.getArray("colors").size());
    assertEquals("red", user.getArray("colors").get(0));
    assertEquals("blue", user.getArray("colors").get(1));
    assertEquals(true, user.getBoolean("active"));

    assertEquals("Jane Doe", child0.getString("name"));
    assertEquals("Tom Doe", child1.getString("name"));
  }

}
