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
 * Represents a {@link com.couchbase.client.java.document.Document} that contains a {@link java.lang.String} as the content.
 *
 * Note that there is no public constructor available, but rather a multitude of factory methods that allow you to work
 * nicely with this immutable value object. It is possible to construct empty/fresh ones, but also copies will be
 * created from passed in documents, allowing you to override specific parts during the copy process.
 *
 * It can always be the case that some or all fields of a {@link com.couchbase.client.java.document.StringDocument} are not set, depending on the operation
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
public class StringDocument extends AbstractDocument<String> {

    /**
     * Creates a empty {@link com.couchbase.client.java.document.StringDocument}.
     *
     * @return a empty {@link com.couchbase.client.java.document.StringDocument}.
     */
    public static StringDocument empty() {
        return new StringDocument(null, null, 0, 0, null);
    }

    /**
     * Creates a {@link com.couchbase.client.java.document.StringDocument} which the document id.
     *
     * @param id the per-bucket unique document id.
     * @return a {@link com.couchbase.client.java.document.StringDocument}.
     */
    public static StringDocument create(final String id) {
        return new StringDocument(id, null, 0, 0, null);
    }

    /**
     * Creates a {@link com.couchbase.client.java.document.StringDocument} which the document id and JSON content.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @return a {@link com.couchbase.client.java.document.StringDocument}.
     */
    public static StringDocument create(final String id, final String content) {
        return new StringDocument(id, content, 0, 0, null);
    }

    /**
     * Creates a {@link com.couchbase.client.java.document.StringDocument} which the document id, JSON content and the CAS value.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @return a {@link com.couchbase.client.java.document.StringDocument}.
     */
    public static StringDocument create(final String id, final String content, final long cas) {
        return new StringDocument(id, content, cas, 0, null);
    }

    /**
     * Creates a {@link com.couchbase.client.java.document.StringDocument} which the document id, JSON content and the expiration time.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param expiry the expiration time of the document.
     * @return a {@link com.couchbase.client.java.document.StringDocument}.
     */
    public static StringDocument create(final String id, final String content, final int expiry) {
        return new StringDocument(id, content, 0, expiry, null);
    }

    /**
     * Creates a {@link com.couchbase.client.java.document.StringDocument} which the document id, JSON content, CAS value, expiration time and status code.
     *
     * This factory method is normally only called within the client library when a response is analyzed and a document
     * is returned which is enriched with the status code. It does not make sense to pre populate the status field from
     * the user level code.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     * @param status the response status as returned by the underlying infrastructure.
     * @return a {@link com.couchbase.client.java.document.StringDocument}.
     */
    public static StringDocument create(final String id, final String content, final long cas, final int expiry, final ResponseStatus status) {
        return new StringDocument(id, content, cas, expiry, status);
    }

    /**
     * Creates a copy from a different {@link com.couchbase.client.java.document.StringDocument}, but changes the document ID.
     *
     * @param doc the original {@link com.couchbase.client.java.document.StringDocument} to copy.
     * @param id the per-bucket unique document id.
     * @return a copied {@link com.couchbase.client.java.document.StringDocument} with the changed properties.
     */
    public static StringDocument fromId(final StringDocument doc, final String id) {
        return StringDocument.create(id, doc.content(), doc.cas(), doc.expiry(), doc.status());
    }

    /**
     * Creates a copy from a different {@link com.couchbase.client.java.document.StringDocument}, but changes the content.
     *
     * @param doc the original {@link com.couchbase.client.java.document.StringDocument} to copy.
     * @param content the content of the document.
     * @return a copied {@link com.couchbase.client.java.document.StringDocument} with the changed properties.
     */
    public static StringDocument from(final StringDocument doc, final String content) {
        return StringDocument.create(doc.id(), content, doc.cas(), doc.expiry(), doc.status());
    }

    /**
     * Creates a copy from a different {@link com.couchbase.client.java.document.StringDocument}, but changes the document ID and content.
     *
     * @param doc the original {@link com.couchbase.client.java.document.StringDocument} to copy.
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @return a copied {@link com.couchbase.client.java.document.StringDocument} with the changed properties.
     */
    public static StringDocument from(final StringDocument doc, final String id, final String content) {
        return StringDocument.create(id, content, doc.cas(), doc.expiry(), doc.status());
    }

    /**
     * Creates a copy from a different {@link com.couchbase.client.java.document.StringDocument}, but changes the CAS value.
     *
     * @param doc the original {@link com.couchbase.client.java.document.StringDocument} to copy.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @return a copied {@link com.couchbase.client.java.document.StringDocument} with the changed properties.
     */
    public static StringDocument from(final StringDocument doc, final long cas) {
        return StringDocument.create(doc.id(), doc.content(), cas, doc.expiry(), doc.status());
    }

    /**
     * Private constructor which is called by the static factory methods eventually.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     * @param status the response status as returned by the underlying infrastructure.
     */
    private StringDocument(final String id, final String content, final long cas, final int expiry, final ResponseStatus status) {
        super(id, content, cas, expiry, status);
    }

}
