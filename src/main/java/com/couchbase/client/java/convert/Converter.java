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
import com.couchbase.client.java.document.Document;

/**
 * Generic interface for document body converters.
 */
public interface Converter {

  /**
   * Converts decode the source format into a {@link CoreDocument}.
   *
   * @param document the document with the source content.
   * @return the converted core document.
   */
  <D extends Document<?>> CoreDocument encode(D document);

  /**
   * Converts decode a {@link CoreDocument} into the target format.
   *
   * @param coreDocument the core document to be decoded.
   * @return the converted object.
   */
  <D extends Document<?>> D decode(CoreDocument coreDocument);
}
