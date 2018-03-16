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

import com.couchbase.client.java.query.Stale;
import com.couchbase.client.java.query.ViewQuery;
import com.couchbase.client.java.query.ViewResult;
import com.couchbase.client.java.util.TestProperties;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observer;

import java.util.concurrent.CountDownLatch;

public class ViewTest {

  private static final String seedNode = TestProperties.seedNode();
  private static final String bucketName = TestProperties.bucket();
  private static final String password = TestProperties.password();

  private static Bucket bucket;

  @BeforeClass
  public static void connect() {
    CouchbaseCluster cluster = new CouchbaseCluster(seedNode);
    bucket = cluster
      .openBucket(bucketName, password)
      .toBlocking()
      .single();
  }

  @Test
  public void shouldQueryView() throws Exception {
      while (true) {
          final CountDownLatch latch = new CountDownLatch(100);
          for (int i = 0; i < 100; i++) {
              bucket.query(ViewQuery.from("foo", "bar").stale(Stale.TRUE)).subscribe(new Observer<ViewResult>() {
                  @Override
                  public void onCompleted() {
                      latch.countDown();
                  }

                  @Override
                  public void onError(Throwable e) {
                      //System.out.println(e);
                  }

                  @Override
                  public void onNext(ViewResult viewRow) {
                      //System.out.println(viewRow.id());
                  }
              });
          }
          latch.await();
      }
  }
}
