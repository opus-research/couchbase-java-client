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
import com.couchbase.client.java.convert.util.BaseSerializer;
import com.couchbase.client.java.convert.util.StringUtils;
import com.couchbase.client.java.convert.util.TranscoderUtils;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.util.Date;

/**
 * Converter for {@link java.lang.Object}s.
 *
 * @author David Sondermann
 * @since 2.0
 */
public class BinaryConverter extends BaseSerializer implements Converter {

  // General flags
  private static final int SERIALIZED_OBJECT = 1;
  private static final int COMPRESSED = 2;

  // Flags for serialized types
  private static final int SPECIAL_BOOLEAN = (1 << 8);
  private static final int SPECIAL_INT = (2 << 8);
  private static final int SPECIAL_LONG = (3 << 8);
  private static final int SPECIAL_DATE = (4 << 8);
  private static final int SPECIAL_BYTE = (5 << 8);
  private static final int SPECIAL_FLOAT = (6 << 8);
  private static final int SPECIAL_DOUBLE = (7 << 8);
  private static final int SPECIAL_BYTE_ARRAY = (8 << 8);

  private static final int SPECIAL_MASK = 0xff00;

  private final TranscoderUtils tu = new TranscoderUtils(true);

  @Override
  public CoreDocument encode(final Document document) {
    final Object content = document.content();
    int flags = 0;
    byte[] data;
    boolean compressionAllowed = true;

    // Encode special types
    if (content instanceof String) {
      final String contentString = (String) content;
      data = contentString.getBytes(CharsetUtil.UTF_8);
      if (StringUtils.isJsonObject(contentString)) {
        compressionAllowed = false;
      }
    } else if (content instanceof Boolean) {
      flags |= SPECIAL_BOOLEAN;
      data = tu.encodeBoolean((Boolean) content);
    } else if (content instanceof Integer) {
      flags |= SPECIAL_INT;
      data = tu.encodeInt((Integer) content);
    } else if (content instanceof Long) {
      flags |= SPECIAL_LONG;
      data = tu.encodeLong((Long) content);
    } else if (content instanceof Date) {
      flags |= SPECIAL_DATE;
      data = tu.encodeLong(((Date) content).getTime());
    } else if (content instanceof Byte) {
      flags |= SPECIAL_BYTE;
      data = tu.encodeByte((Byte) content);
    } else if (content instanceof Float) {
      flags |= SPECIAL_FLOAT;
      data = tu.encodeInt(Float.floatToRawIntBits((Float) content));
    } else if (content instanceof Double) {
      flags |= SPECIAL_DOUBLE;
      data = tu.encodeLong(Double.doubleToRawLongBits((Double) content));
    } else if (content instanceof byte[]) {
      flags |= SPECIAL_BYTE_ARRAY;
      data = (byte[]) content;
    } else {
      // Serialize all other objects
      flags |= SERIALIZED_OBJECT;
      data = encodeObject(content);
    }

    // Compress data
    if (compressionAllowed && data.length > DEFAULT_COMPRESSION_THRESHOLD) {
      final byte[] compressed = compress(data);
      if (compressed.length < data.length) {
        flags |= COMPRESSED;
        data = compressed;
      }
    }

    // TODO check if we should set the isJson flag to TRUE if String content is a json object
    return new CoreDocument(document.id(), Unpooled.copiedBuffer(data), flags, document.expiry(), document.cas(), false, document.status());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D extends Document<?>> D decode(final CoreDocument coreDocument) {
    if (coreDocument.status() != ResponseStatus.SUCCESS)
    {
      return (D) BinaryDocument.create(coreDocument.id(), null, coreDocument.cas(), coreDocument.expiration(), coreDocument.status());
    }

    final boolean isCompressed = ((coreDocument.flags() & COMPRESSED) != 0);
    final byte[] data = isCompressed ? decompress(coreDocument.content().array()) : coreDocument.content().array();

    if ((coreDocument.flags() & SERIALIZED_OBJECT) != 0) {
      return (D) BinaryDocument.create(coreDocument.id(), decodeObject(data), coreDocument.cas(), coreDocument.expiration(), coreDocument.status());
    }

    Object content;
    switch (coreDocument.flags() & SPECIAL_MASK) {
      case SPECIAL_BOOLEAN:
        content = tu.decodeBoolean(data);
        break;
      case SPECIAL_INT:
        content = tu.decodeInt(data);
        break;
      case SPECIAL_LONG:
        content = tu.decodeLong(data);
        break;
      case SPECIAL_DATE:
        content = new Date(tu.decodeLong(data));
        break;
      case SPECIAL_BYTE:
        content = tu.decodeByte(data);
        break;
      case SPECIAL_FLOAT:
        content = Float.intBitsToFloat(tu.decodeInt(data));
        break;
      case SPECIAL_DOUBLE:
        content = Double.longBitsToDouble(tu.decodeLong(data));
        break;
      case SPECIAL_BYTE_ARRAY:
        content = data;
        break;
      default:
        content = (data != null) ? new String(data, CharsetUtil.UTF_8) : null;
    }

    return (D) BinaryDocument.create(coreDocument.id(), content, coreDocument.cas(), coreDocument.expiration(), coreDocument.status());
  }
}
