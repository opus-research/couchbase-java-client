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
import com.couchbase.client.java.convert.util.BaseSerializer;
import com.couchbase.client.java.convert.util.StringUtils;
import com.couchbase.client.java.convert.util.TranscoderUtils;
import com.couchbase.client.java.document.BinaryDocument;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.util.Date;

/**
 * Converter for {@link java.lang.Object}s.
 *
 * @author David Sondermann
 * @since 2.0
 */
public class BinaryConverter extends BaseSerializer implements Converter<BinaryDocument, Object> {

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
  public Object decode(final CachedData cachedData) {
    return decode(cachedData.getBuffer(), cachedData.getFlags());
  }

  @Override
  public Object decode(final ByteBuf buffer, final int flags) {
    final boolean isCompressed = ((flags & COMPRESSED) != 0);
    final byte[] data = isCompressed ? decompress(buffer.array()) : buffer.array();

    if ((flags & SERIALIZED_OBJECT) != 0) {
      return decodeObject(data);
    }

    switch (flags & SPECIAL_MASK) {
      case SPECIAL_BOOLEAN:
        return tu.decodeBoolean(data);
      case SPECIAL_INT:
        return tu.decodeInt(data);
      case SPECIAL_LONG:
        return tu.decodeLong(data);
      case SPECIAL_DATE:
        return new Date(tu.decodeLong(data));
      case SPECIAL_BYTE:
        return tu.decodeByte(data);
      case SPECIAL_FLOAT:
        return Float.intBitsToFloat(tu.decodeInt(data));
      case SPECIAL_DOUBLE:
        return Double.longBitsToDouble(tu.decodeLong(data));
      case SPECIAL_BYTE_ARRAY:
        return data;
      default:
        return (data != null) ? new String(data, CharsetUtil.UTF_8) : null;
    }
  }

  @Override
  public BinaryDocument newDocument(final String id, final Object content, final long cas, final int expiry, final ResponseStatus status) {
    return BinaryDocument.create(id, content, cas, expiry, status);
  }

  @Override
  public CachedData encode(final Object content) {
    int flags = 0;
    byte[] data;
    boolean compressionAllowed = true;

    // Encode special types
    if (content instanceof String) {
      final String document = (String) content;
      data = (document).getBytes(CharsetUtil.UTF_8);
      if (StringUtils.isJsonObject(document)) {
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

    return new CachedData(flags, Unpooled.copiedBuffer(data));
  }
}
