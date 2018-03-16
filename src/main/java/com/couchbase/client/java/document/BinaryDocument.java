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
package com.couchbase.client.java.document;

import com.couchbase.client.core.message.ResponseStatus;

/**
 * Represents a {@link com.couchbase.client.java.document.Document} that contains a {@link java.lang.Object} as the content.
 *
 * Note that there is no public constructor available, but rather a multitude of factory methods that allow you to work
 * nicely with this immutable value object. It is possible to construct empty/fresh ones, but also copies will be
 * created from passed in documents, allowing you to override specific parts during the copy process.
 *
 * It can always be the case that some or all fields of a {@link BinaryDocument} are not set, depending on the operation
 * performed. Here are the accessible fields and their default values:
 *
 * +---------+---------+
 * | Field   | Default |
 * +---------+---------+
 * | id      | null    |
 * | content | null    |
 * | cas     | 0       |
 * | expiry  | 0       |
 * | status  | null    |
 * +---------+---------+
 *
 * @author David Sondermann
 * @since 2.0
 */
public class BinaryDocument extends AbstractDocument<Object> {

  /**
   * Creates a empty {@link BinaryDocument}.
   *
   * @return a empty {@link BinaryDocument}.
   */
  public static BinaryDocument empty() {
    return new BinaryDocument(null, null, 0, 0, null);
  }

  /**
   * Creates a {@link BinaryDocument} which the document id.
   *
   * @param id the per-bucket unique document id.
   * @return a {@link BinaryDocument}.
   */
  public static BinaryDocument create(final String id) {
    return new BinaryDocument(id, null, 0, 0, null);
  }

  /**
   * Creates a {@link BinaryDocument} which the document id and JSON content.
   *
   * @param id      the per-bucket unique document id.
   * @param content the content of the document.
   * @return a {@link BinaryDocument}.
   */
  public static BinaryDocument create(final String id, final Object content) {
    return new BinaryDocument(id, content, 0, 0, null);
  }

  /**
   * Creates a {@link BinaryDocument} which the document id, JSON content and the CAS value.
   *
   * @param id      the per-bucket unique document id.
   * @param content the content of the document.
   * @param cas     the CAS (compare and swap) value for optimistic concurrency.
   * @return a {@link BinaryDocument}.
   */
  public static BinaryDocument create(final String id, final Object content, final long cas) {
    return new BinaryDocument(id, content, cas, 0, null);
  }

  /**
   * Creates a {@link BinaryDocument} which the document id, JSON content and the expiration time.
   *
   * @param id      the per-bucket unique document id.
   * @param content the content of the document.
   * @param expiry  the expiration time of the document.
   * @return a {@link BinaryDocument}.
   */
  public static BinaryDocument create(final String id, final Object content, final int expiry) {
    return new BinaryDocument(id, content, 0, expiry, null);
  }

  /**
   * Creates a {@link BinaryDocument} which the document id, JSON content, CAS value, expiration time and status code.
   *
   * This factory method is normally only called within the client library when a response is analyzed and a document
   * is returned which is enriched with the status code. It does not make sense to pre populate the status field from
   * the user level code.
   *
   * @param id      the per-bucket unique document id.
   * @param content the content of the document.
   * @param cas     the CAS (compare and swap) value for optimistic concurrency.
   * @param expiry  the expiration time of the document.
   * @param status  the response status as returned by the underlying infrastructure.
   * @return a {@link BinaryDocument}.
   */
  public static BinaryDocument create(final String id, final Object content, final long cas, final int expiry, final ResponseStatus status) {
    return new BinaryDocument(id, content, cas, expiry, status);
  }

  /**
   * Creates a copy from a different {@link BinaryDocument}, but changes the document ID.
   *
   * @param doc the original {@link BinaryDocument} to copy.
   * @param id  the per-bucket unique document id.
   * @return a copied {@link BinaryDocument} with the changed properties.
   */
  public static BinaryDocument fromId(final BinaryDocument doc, final String id) {
    return BinaryDocument.create(id, doc.content(), doc.cas(), doc.expiry(), doc.status());
  }

  /**
   * Creates a copy from a different {@link BinaryDocument}, but changes the content.
   *
   * @param doc     the original {@link BinaryDocument} to copy.
   * @param content the content of the document.
   * @return a copied {@link BinaryDocument} with the changed properties.
   */
  public static BinaryDocument from(final BinaryDocument doc, final Object content) {
    return BinaryDocument.create(doc.id(), content, doc.cas(), doc.expiry(), doc.status());
  }

  /**
   * Creates a copy from a different {@link BinaryDocument}, but changes the document ID and content.
   *
   * @param doc     the original {@link BinaryDocument} to copy.
   * @param id      the per-bucket unique document id.
   * @param content the content of the document.
   * @return a copied {@link BinaryDocument} with the changed properties.
   */
  public static BinaryDocument from(final BinaryDocument doc, final String id, final Object content) {
    return BinaryDocument.create(id, content, doc.cas(), doc.expiry(), doc.status());
  }

  /**
   * Creates a copy from a different {@link BinaryDocument}, but changes the CAS value.
   *
   * @param doc the original {@link BinaryDocument} to copy.
   * @param cas the CAS (compare and swap) value for optimistic concurrency.
   * @return a copied {@link BinaryDocument} with the changed properties.
   */
  public static BinaryDocument from(final BinaryDocument doc, final long cas) {
    return BinaryDocument.create(doc.id(), doc.content(), cas, doc.expiry(), doc.status());
  }

  @Override
  public Document<Object> copy(final long cas, final ResponseStatus status) {
    return new BinaryDocument(id(), content(), cas, expiry(), status);
  }

  /**
   * Private constructor which is called by the static factory methods eventually.
   *
   * @param id      the per-bucket unique document id.
   * @param content the content of the document.
   * @param cas     the CAS (compare and swap) value for optimistic concurrency.
   * @param expiry  the expiration time of the document.
   * @param status  the response status as returned by the underlying infrastructure.
   */
  private BinaryDocument(final String id, final Object content, final long cas, final int expiry, final ResponseStatus status) {
    super(id, content, cas, expiry, status);
  }

}
