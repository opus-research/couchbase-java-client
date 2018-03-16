package com.couchbase.client.java.convert;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

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
    final CachedData cachedData = converter.encode(object);

    assertEquals("{}", cachedData.getBuffer().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeEmptyJsonObject() {
    final ByteBuf buf = Unpooled.copiedBuffer("{}", CharsetUtil.UTF_8);
    final JsonObject object = converter.decode(buf, 0);

    assertTrue(object.isEmpty());
  }

  @Test
  public void shouldEncodeEmptyJsonArray() {
    final JsonObject object = JsonObject.empty();
    object.put("array", JsonArray.empty());
    final CachedData cachedData = converter.encode(object);

    assertEquals("{\"array\":[]}", cachedData.getBuffer().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeEmptyJsonArray() {
    final ByteBuf buf = Unpooled.copiedBuffer("{\"array\":[]}", CharsetUtil.UTF_8);
    final JsonObject object = converter.decode(buf, 0);

    assertFalse(object.isEmpty());
    assertTrue(object.getArray("array") instanceof JsonArray);
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

    final CachedData cachedData = converter.encode(object);
    String expected = "{\"integer\":1,\"string\":\"Hello World\",\"boolean\":" +
        "true,\"double\":11.3322,\"long\":9223372036854775807}";

    assertEquals(expected, cachedData.getBuffer().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeSimpleJsonObject() {
    final String input = "{\"integer\":1,\"string\":\"Hello World\",\"boolean\":" +
        "true,\"double\":11.3322,\"long\":9223372036854775807}";
    final ByteBuf buf = Unpooled.copiedBuffer(input, CharsetUtil.UTF_8);
    final JsonObject object = converter.decode(buf, 0);

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

    final CachedData cachedData = converter.encode(JsonObject.empty().put("array", array));
    final String expected = "{\"array\":[\"Hello World\",1,9223372036854775807,11.3322,false]}";

    assertEquals(expected, cachedData.getBuffer().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeSimpleJsonArray() {
    final String input = "{\"array\":[\"Hello World\",1,9223372036854775807,11.3322,false]}";
    final ByteBuf buf = Unpooled.copiedBuffer(input, CharsetUtil.UTF_8);
    final JsonObject object = converter.decode(buf, 0);
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
    final CachedData cachedData = converter.encode(object);
    final String expected = "{\"object\":{\"inner\":{\"foo\":\"bar\"}}}";

    assertEquals(expected, cachedData.getBuffer().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeNestedJsonObjects() {
    final String input = "{\"object\":{\"inner\":{\"foo\":\"bar\"}}}";
    final ByteBuf buf = Unpooled.copiedBuffer(input, CharsetUtil.UTF_8);
    final JsonObject object = converter.decode(buf, 0);

    assertEquals(1, object.size());
    assertEquals(1, object.getObject("object").size());
    assertEquals("bar", object.getObject("object").getObject("inner").get("foo"));
  }

  @Test
  public void shouldEncodeNestedJsonArrays() {
    final CachedData cachedData = converter.encode(JsonObject.empty().put("inner", JsonArray.empty().add(JsonArray.empty())));
    final String expected = "{\"inner\":[[]]}";

    assertEquals(expected, cachedData.getBuffer().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeNestedJsonArray() {
    final String input = "{\"inner\":[[]]}";
    final ByteBuf buf = Unpooled.copiedBuffer(input, CharsetUtil.UTF_8);
    final JsonObject object = converter.decode(buf, 0);

    assertEquals(1, object.size());
    assertEquals(1, object.getArray("inner").size());
    assertTrue(object.getArray("inner").getArray(0).isEmpty());
  }

  @Test
  public void shouldEncodeMixedNestedJsonValues() {
    final JsonArray children = JsonArray.empty()
        .add(JsonObject.empty().put("name", "Jane Doe").put("age", 25))
        .add(JsonObject.empty().put("name", "Tom Doe").put("age", 13));

    final JsonObject user = JsonObject.empty()
        .put("firstname", "John")
        .put("lastname", "Doe")
        .put("colors", JsonArray.empty().add("red").add("blue"))
        .put("children", children)
        .put("active", true);

    final String expected = "{\"colors\":[\"red\",\"blue\"],\"active\":true," +
        "\"children\":[{\"age\":25,\"name\":\"Jane Doe\"},{\"age\":13,\"name\":" +
        "\"Tom Doe\"}],\"lastname\":\"Doe\",\"firstname\":\"John\"}";
    final CachedData cachedData = converter.encode(user);

    assertEquals(expected, cachedData.getBuffer().toString(CharsetUtil.UTF_8));
  }

  @Test
  public void shouldDecodeMixedNestedJsonValues() {
    final String input = "{\"colors\":[\"red\",\"blue\"],\"active\":true," +
        "\"children\":[{\"age\":25,\"name\":\"Jane Doe\"},{\"age\":13,\"name\":" +
        "\"Tom Doe\"}],\"lastname\":\"Doe\",\"firstname\":\"John\"}";
    final ByteBuf buf = Unpooled.copiedBuffer(input, CharsetUtil.UTF_8);
    final JsonObject user = converter.decode(buf, 0);
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
