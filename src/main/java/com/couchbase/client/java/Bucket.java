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
package com.couchbase.client.java;

import com.couchbase.client.core.message.config.FlushResponse;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ViewQuery;
import com.couchbase.client.java.query.ViewResult;
import rx.Observable;

/**
 * Represents a Couchbase Server bucket.
 *
 * @author Michael Nitschinger
 * @author David Sondermann
 * @since 2.0
 */
public interface Bucket {

  /**
   * Get a {@link Document} by its unique ID.
   *
   * The loaded document will be converted using the default converter, which is
   * JSON if not configured otherwise.
   *
   * @param id the ID of the document.
   * @return the loaded and converted document.
   */
  Observable<JsonDocument> get(String id);

  /**
   * Get a {@link Document} by its unique ID.
   *
   * The loaded document will be converted into the target class, which needs
   * a custom converter registered with the system.
   *
   * @param id     the ID of the document.
   * @param target the document type.
   * @return the loaded and converted document.
   */
  <D extends Document<?>> Observable<D> get(String id, Class<D> target);

  /**
   * Insert a {@link Document}.
   *
   * @param document the document to insert.
   * @param <D>      the type of the document, which is inferred from the instance.
   * @return the document again.
   */
  <D extends Document<?>> Observable<D> insert(D document);

  /**
   * Upsert a {@link Document}.
   *
   * @param document the document to upsert.
   * @param <D>      the type of the document, which is inferred from the instance.
   * @return the document again.
   */
  <D extends Document<?>> Observable<D> upsert(D document);

  /**
   * Replace a {@link Document}.
   *
   * @param document the document to replace.
   * @param <D>      the type of the document, which is inferred from the instance.
   * @return the document again.
   */
  <D extends Document<?>> Observable<D> replace(D document);

  /**
   * Remove the given {@link Document}.
   *
   * @param document the document to remove.
   * @param <D> the type of the document, which is inferred from the instance.
   * @return the document again.
   */
  <D extends Document<?>> Observable<D> remove(D document);

  /**
   * Remove the document by the given document ID.
   *
   * @param id the ID of the document.
   * @return the document again.
   */
  Observable<JsonDocument> remove(String id);

  /**
   * Queries a View defined by the {@link ViewQuery} and returns a {@link ViewResult}
   * for each emitted row in the view.
   *
   * @param query the query for the view.
   * @return a row for each result (from 0 to N).
   */
  Observable<ViewResult> query(ViewQuery query);

  /**
   * Runs a {@link Query} and returns a {@link QueryResult} for each emitted row in the result.
   *
   * @param query the query.
   * @return a row for each result (from 0 to N).
   */
  Observable<QueryResult> query(Query query);

  /**
   * Runs a raw N1QL query and returns a {@link QueryResult} for each emitted row in the result.
   *
   * @param query the query.
   * @return the query result.
   */
  Observable<QueryResult> query(String query);

  /**
   * Flushes the bucket (has to be enabled in the bucket configuration).
   *
   * @return the flush response.
   */
  Observable<FlushResponse> flush();
}
