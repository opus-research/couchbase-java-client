package com.couchbase.client.java.convert;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.java.document.Document;
import io.netty.buffer.ByteBuf;

/**
 * Generic interface for document body converters.
 */
public interface Converter<D extends Document, T> {

  /**
   * Creates a new and empty document.
   *
   * @return a new document.
   */
  D newDocument(String id, T content, long cas, int expiry, ResponseStatus status);

  /**
   * Converts decode a {@link CachedData} into the target format.
   *
   * @param cachedData the cached data to be decoded.
   * @return the converted object.
   */
  T decode(CachedData cachedData);

  /**
   * Converts decode a {@link ByteBuf} into the target format.
   *
   * @param buffer the buffer to be decodede.
   * @param flags the flags to decode the ByteBuffer
   * @return the converted object.
   */
  T decode(ByteBuf buffer, int flags);

  /**
   * Converts decode the source format into a {@link CachedData}.
   *
   * @param content the source content.
   * @return the converted cached data.
   */
  CachedData encode(T content);
}
