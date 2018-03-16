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

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.document.CoreDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.io.IOException;

/**
 * Converter for {@link JsonObject}s.
 */
public class JacksonJsonConverter implements Converter {

  /**
   * The internal jackson object mapper.
   */
  private final ObjectMapper mapper;

  public JacksonJsonConverter() {
    final SimpleModule module = new SimpleModule("JsonValueModule", new Version(1, 0, 0, null, null, null));
    module.addSerializer(JsonObject.class, new JsonObjectSerializer());
    module.addSerializer(JsonArray.class, new JsonArraySerializer());
    module.addDeserializer(JsonObject.class, new JsonObjectDeserializer());

    mapper = new ObjectMapper();
    mapper.registerModule(module);
  }

  @Override
  public CoreDocument encode(final Document document) {
    try {
      final ByteBuf content = Unpooled.copiedBuffer(mapper.writeValueAsString(document.content()), CharsetUtil.UTF_8);
      // TODO check if we should set the isJson flag to TRUE for JsonDocuments
      return new CoreDocument(document.id(), content, 0, document.expiry(), document.cas(), false, document.status());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D extends Document<?>> D decode(final CoreDocument coreDocument) {
    if (coreDocument.status() != ResponseStatus.SUCCESS)
    {
      return (D) JsonDocument.create(coreDocument.id(), null, coreDocument.cas(), coreDocument.expiration(), coreDocument.status());
    }

    try {
      final JsonObject content = mapper.readValue(coreDocument.content().toString(CharsetUtil.UTF_8), JsonObject.class);
      return (D) JsonDocument.create(coreDocument.id(), content, coreDocument.cas(), coreDocument.expiration(), coreDocument.status());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public JsonObject decode(final String content) {
    try {
      return mapper.readValue(content, JsonObject.class);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static class JsonObjectSerializer extends JsonSerializer<JsonObject> {
    @Override
    public void serialize(final JsonObject value, final JsonGenerator jsonGenerator, final SerializerProvider provider)
        throws IOException {
      jsonGenerator.writeObject(value.toMap());
    }
  }

  static class JsonArraySerializer extends JsonSerializer<JsonArray> {
    @Override
    public void serialize(final JsonArray value, final JsonGenerator jsonGenerator, final SerializerProvider provider)
        throws IOException {
      jsonGenerator.writeObject(value.toList());
    }
  }

  static class JsonObjectDeserializer extends JsonDeserializer<JsonObject> {
    @Override
    public JsonObject deserialize(final JsonParser parser, final DeserializationContext ctx) throws IOException {
      if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
        return decodeObject(parser, JsonObject.empty());
      } else {
        throw new IllegalStateException("Expecting Object as root level object, " +
            "was: " + parser.getCurrentToken());
      }
    }

    private JsonObject decodeObject(final JsonParser parser, final JsonObject target) throws IOException {
      JsonToken current = parser.nextToken();
      String field = null;

      while (current != null && current != JsonToken.END_OBJECT) {
        if (current == JsonToken.START_OBJECT) {
          target.put(field, decodeObject(parser, JsonObject.empty()));
        } else if (current == JsonToken.START_ARRAY) {
          target.put(field, decodeArray(parser, JsonArray.empty()));
        } else if (current == JsonToken.FIELD_NAME) {
          field = parser.getCurrentName();
        } else {
          switch (current) {
            case VALUE_TRUE:
            case VALUE_FALSE:
              target.put(field, parser.getValueAsBoolean());
              break;
            case VALUE_STRING:
              target.put(field, parser.getValueAsString());
              break;
            case VALUE_NUMBER_INT:
              try {
                target.put(field, parser.getValueAsInt());
              } catch (final JsonParseException e) {
                target.put(field, parser.getValueAsLong());
              }
              break;
            case VALUE_NUMBER_FLOAT:
              target.put(field, parser.getValueAsDouble());
              break;
            case VALUE_NULL:
              target.put(field, (JsonObject) null);
              break;
            default:
              throw new IllegalStateException("Could not decode JSON token: " + current);
          }
        }

        current = parser.nextToken();
      }

      return target;
    }

    private JsonArray decodeArray(final JsonParser parser, final JsonArray target) throws IOException {
      JsonToken current = parser.nextToken();

      while (current != null && current != JsonToken.END_ARRAY) {
        if (current == JsonToken.START_OBJECT) {
          target.add(decodeObject(parser, JsonObject.empty()));
        } else if (current == JsonToken.START_ARRAY) {
          target.add(decodeArray(parser, JsonArray.empty()));
        } else {
          switch (current) {
            case VALUE_TRUE:
            case VALUE_FALSE:
              target.add(parser.getValueAsBoolean());
              break;
            case VALUE_STRING:
              target.add(parser.getValueAsString());
              break;
            case VALUE_NUMBER_INT:
              try {
                target.add(parser.getValueAsInt());
              } catch (final JsonParseException e) {
                target.add(parser.getValueAsLong());
              }
              break;
            case VALUE_NUMBER_FLOAT:
              target.add(parser.getValueAsDouble());
              break;
            case VALUE_NULL:
              target.add((JsonObject) null);
              break;
            default:
              throw new IllegalStateException("Could not decode JSON token.");
          }
        }

        current = parser.nextToken();
      }

      return target;
    }

  }

}
