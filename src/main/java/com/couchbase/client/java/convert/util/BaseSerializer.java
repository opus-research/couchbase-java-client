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

package com.couchbase.client.java.convert.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Base class for any converters that may want to work with serialized or compressed data.
 */
public abstract class BaseSerializer {

  protected static final int DEFAULT_COMPRESSION_THRESHOLD = 16384;

  /**
   * Get the bytes representing the given serialized object.
   */
  protected byte[] encodeObject(final Object content) {
    if (content == null) {
      throw new NullPointerException("Can't serialize null");
    }

    ByteArrayOutputStream bos = null;
    ObjectOutputStream os = null;

    try {
      bos = new ByteArrayOutputStream();
      os = new ObjectOutputStream(bos);
      os.writeObject(content);
      os.close();
      bos.close();

      return bos.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException("Non-serializable object", e);
    } finally {
      close(os);
      close(bos);
    }
  }

  /**
   * Get the object represented by the given serialized bytes.
   */
  protected Object decodeObject(final byte[] data) {
    if (data == null) {
      return null;
    }

    ByteArrayInputStream bis = null;
    ObjectInputStream is = null;

    try {
      bis = new ByteArrayInputStream(data);
      is = new ObjectInputStream(bis);

      return is.readObject();
    } catch (IOException e) {
      throw new IllegalArgumentException("IOException on decoding object with " + data.length + " bytes", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("ClassNotFoundException on on decoding object with " + data.length + " bytes", e);
    } finally {
      close(is);
      close(bis);
    }
  }

  /**
   * Compress the given array of bytes.
   */
  protected byte[] compress(byte[] uncompressedData) {
    if (uncompressedData == null) {
      throw new NullPointerException("Can't compress null");
    }

    ByteArrayOutputStream outputStream = null;
    GZIPOutputStream gzipOutputStream = null;

    try {
      outputStream = new ByteArrayOutputStream();
      gzipOutputStream = new GZIPOutputStream(outputStream);
      gzipOutputStream.write(uncompressedData);

      gzipOutputStream.flush();
      gzipOutputStream.close();
      outputStream.close();

      return outputStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("IO exception compressing data", e);
    } finally {
      close(gzipOutputStream);
      close(outputStream);
    }
  }

  /**
   * Decompress the given array of bytes.
   *
   * @return null if the bytes cannot be decompressed
   */
  protected byte[] decompress(byte[] compressedData) {
    if (compressedData == null) {
      return null;
    }

    byte[] uncompressed = null;

    ByteArrayInputStream inputStream = null;
    GZIPInputStream gzipInputStream = null;
    ByteArrayOutputStream outputStream = null;

    try {
      inputStream = new ByteArrayInputStream(compressedData);
      gzipInputStream = new GZIPInputStream(inputStream);
      outputStream = new ByteArrayOutputStream();

      byte[] buf = new byte[8192];
      int r;
      while ((r = gzipInputStream.read(buf)) > 0) {
        outputStream.write(buf, 0, r);
      }

      uncompressed = outputStream.toByteArray();
    } catch (IOException ignored) {
    } finally {
      close(outputStream);
      close(gzipInputStream);
      close(inputStream);
    }

    return uncompressed;
  }

  private void close(final Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception ignored) {
      }
    }
  }
}
