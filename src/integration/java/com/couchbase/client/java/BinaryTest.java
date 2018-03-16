package com.couchbase.client.java;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.util.TestProperties;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import rx.observables.BlockingObservable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BinaryTest {

  private static final String seedNode = TestProperties.seedNode();
  private static final String bucketName = TestProperties.bucket();
  private static final String password = TestProperties.password();

  private static Bucket bucket;

  @BeforeClass
  public static void connect() {
    CouchbaseCluster cluster = new CouchbaseCluster(seedNode);
    bucket = cluster
      .openBucket(bucketName, password)
      .toBlockingObservable()
      .single();
  }

  @Test
  public void shouldInsertAndGet() {
    Map<String, String> content = new HashMap<String, String>();
    content.put("hello", "world");
    final Document doc = new JsonDocument("key", content);
    Document response = bucket
      .insert(doc)
      .flatMap(new Func1<Document, Observable<JsonDocument>>() {
        @Override
        public Observable<JsonDocument> call(Document document) {
          return bucket.get("key");
        }
      })
      .toBlockingObservable()
      .single();
    assertEquals(content, response.content());
  }

  @Test
  public void shouldUpsertAndGet() {
    Map<String, String> content = new HashMap<String, String>();
    content.put("hello", "world");
    final Document doc = new JsonDocument("key", content);
    Document response = bucket.upsert(doc)
      .flatMap(new Func1<Document, Observable<JsonDocument>>() {
        @Override
        public Observable<JsonDocument> call(Document document) {
          return bucket.get("key");
        }
      })
      .toBlockingObservable()
      .single();
    assertEquals(content, response.content());
  }

  @Test
  public void shouldLoadMultipleDocuments() throws Exception {
    BlockingObservable<JsonDocument> observable = Observable
      .from("doc1", "doc2", "doc3")
      .flatMap(new Func1<String, Observable<JsonDocument>>() {
        @Override
        public Observable<JsonDocument> call(String id) {
          return bucket.get(id);
        }
      })
      .toBlockingObservable();

    Iterator<JsonDocument> iterator = observable.getIterator();
    while (iterator.hasNext()) {
      Document doc = iterator.next();
      assertEquals(null, doc.content());
    }
  }

}
