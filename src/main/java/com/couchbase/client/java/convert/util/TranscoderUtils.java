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

/**
 * Utility class for transcoding Java types.
 */
public final class TranscoderUtils {

  private final boolean packZeros;

  /**
   * Get an instance of TranscoderUtils.
   *
   * @param pack if true, remove all zero bytes from the MSB of the packed num
   */
  public TranscoderUtils(boolean pack) {
    super();
    packZeros = pack;
  }

  public byte[] encodeNum(long number, int maxBytes) {
    byte[] data = new byte[maxBytes];
    for (int i = 0; i < data.length; i++) {
      data[data.length - i - 1] = (byte) ((number >> (8 * i)) & 0xff);
    }
    if (packZeros) {
      int firstNonZeroPos = 0;
      // Just looking for what we can reduce
      while (firstNonZeroPos < data.length && data[firstNonZeroPos] == 0) {
        firstNonZeroPos++;
      }
      if (firstNonZeroPos > 0) {
        byte[] packedData = new byte[data.length - firstNonZeroPos];
        System.arraycopy(data, firstNonZeroPos, packedData, 0, data.length - firstNonZeroPos);
        data = packedData;
      }
    }
    return data;
  }

  public byte[] encodeLong(long l) {
    return encodeNum(l, 8);
  }

  public long decodeLong(byte[] data) {
    long number = 0;
    for (final byte chunk : data) {
      number = (number << 8) | (chunk < 0 ? 256 + chunk : chunk);
    }
    return number;
  }

  public byte[] encodeInt(int number) {
    return encodeNum(number, 4);
  }

  public int decodeInt(byte[] data) {
    assert data.length <= 4 : "Too long to be an int (" + data.length + ") bytes";
    return (int) decodeLong(data);
  }

  public byte[] encodeByte(byte number) {
    return new byte[]{number};
  }

  public byte decodeByte(byte[] data) {
    assert data.length <= 1 : "Too long for a byte";
    return (data.length == 1) ? data[0] : 0;
  }

  public byte[] encodeBoolean(boolean bool) {
    byte[] data = new byte[1];
    data[0] = (byte) (bool ? '1' : '0');
    return data;
  }

  public boolean decodeBoolean(byte[] data) {
    assert data.length == 1 : "Wrong length for a boolean";
    return (data[0] == '1');
  }
}
