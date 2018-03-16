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

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.util.TestProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import rx.observables.BlockingObservable;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BinaryBinaryDocumentTest {

  private static final String seedNode = TestProperties.seedNode();
  private static final String bucketName = TestProperties.bucket();
  private static final String password = TestProperties.password();

  private static Bucket bucket;

  @BeforeClass
  public static void connect() {
    final CouchbaseCluster cluster = new CouchbaseCluster(seedNode);
    bucket = cluster
        .openBucket(bucketName, password)
        .toBlocking()
        .single();

    cleanup();
  }

  @AfterClass
  public static void tearDown() {
    cleanup();
  }

  private static void cleanup() {
    bucket.flush().toBlocking().single();
  }

  @Test
  public void shouldInsertAndGet() {
    final String content = "{\"hello\":\"world\"}";
    final int expiration = 3;
    final BinaryDocument doc = BinaryDocument.create("insert-binary", content, expiration);

    final BinaryDocument response = bucket
        .insert(doc)
        .flatMap(new Func1<BinaryDocument, Observable<BinaryDocument>>() {
          @Override
          public Observable<BinaryDocument> call(final BinaryDocument document) {
            return bucket.get("insert-binary", BinaryDocument.class);
          }
        })
        .toBlocking()
        .single();

    assertEquals(content, response.content());
    assertEquals(expiration, response.expiry());
    assertEquals(ResponseStatus.SUCCESS, response.status());
  }

  @Test
  public void shouldUpsertAndGet() {
    final int content = Integer.MAX_VALUE;
    final BinaryDocument doc = BinaryDocument.create("upsert-binary", content);
    final BinaryDocument response = bucket
        .upsert(doc)
        .flatMap(new Func1<BinaryDocument, Observable<BinaryDocument>>() {
          @Override
          public Observable<BinaryDocument> call(final BinaryDocument document) {
            return bucket.get("upsert-binary", BinaryDocument.class);
          }
        })
        .toBlocking()
        .single();

    assertEquals(content, response.content());
    assertEquals(ResponseStatus.SUCCESS, response.status());
  }

  @Test
  public void shouldUpsertAndReplace() {
    final Long content = 125312341L;
    final String replacedContent = "I am the future!";

    final BinaryDocument doc = BinaryDocument.create("upsert-binary-replace", content);
    final BinaryDocument response = bucket
        .upsert(doc)
        .flatMap(new Func1<BinaryDocument, Observable<BinaryDocument>>() {
          @Override
          public Observable<BinaryDocument> call(final BinaryDocument document) {

            assertEquals(content, document.content());

            return bucket.get("upsert-binary-replace", BinaryDocument.class);
          }
        })
        .toBlocking()
        .single();

    assertEquals(content, response.content());

    final BinaryDocument updated = BinaryDocument.from(response, replacedContent);
    final BinaryDocument replaced = bucket
        .replace(updated)
        .flatMap(new Func1<BinaryDocument, Observable<BinaryDocument>>() {
          @Override
          public Observable<BinaryDocument> call(final BinaryDocument document) {
            return bucket.get("upsert-binary-replace", BinaryDocument.class);
          }
        })
        .toBlocking()
        .single();

    assertEquals(replacedContent, replaced.content());
  }

  @Test
  public void shouldLoadMultipleDocuments() throws Exception {
    final BlockingObservable<BinaryDocument> observable = Observable
        .from("doc1", "doc2", "doc3")
        .flatMap(new Func1<String, Observable<BinaryDocument>>() {
          @Override
          public Observable<BinaryDocument> call(final String id) {
            return bucket.get(id, BinaryDocument.class);
          }
        })
        .toBlocking();

    final Iterator<BinaryDocument> iterator = observable.getIterator();
    while (iterator.hasNext()) {
      final BinaryDocument doc = iterator.next();

      assertNull(doc.content());
      assertEquals(ResponseStatus.NOT_EXISTS, doc.status());
    }
  }

}
