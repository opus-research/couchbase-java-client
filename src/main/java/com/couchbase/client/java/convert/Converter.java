package com.couchbase.client.java.convert;

import io.netty.buffer.ByteBuf;

/**
 * Generic interface for document body converters.
 */
public interface Converter {

  /**
   * Converts from a {@link ByteBuf} into the target format.
   *
   * @param buffer the buffer to convert from.
   * @return the converted object.
   */
  Object from(ByteBuf buffer);

  /**
   * Converts from the source format into a {@link ByteBuf}.
   *
   * @param content the source content.
   * @return the converted byte buffer.
   */
  ByteBuf to(Object content);

}
