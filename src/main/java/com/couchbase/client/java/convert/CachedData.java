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

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * Cached buffer with its attributes.
 *
 * @author David Sondermann
 * @since 2.0
 */
public final class CachedData {

  private final int flags;
  private final ByteBuf buffer;

  /**
   * Get a CachedData instance for the given flags and ByteBuf.
   *
   * @param flags  the flags
   * @param buffer the buffer
   */
  public CachedData(final int flags, final ByteBuf buffer) {
    this.flags = flags;
    this.buffer = buffer;
  }

  /**
   * Get the stored buffer.
   */
  public ByteBuf getBuffer() {
    return buffer;
  }

  /**
   * Get the flags stored along with this value.
   */
  public int getFlags() {
    return flags;
  }

  @Override
  public String toString() {
    return "{CachedData flags=" + flags + " buffer=" + buffer.toString(CharsetUtil.UTF_8) + "}";
  }
}
