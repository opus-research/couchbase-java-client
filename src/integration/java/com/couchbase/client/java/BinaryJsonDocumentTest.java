package com.couchbase.client.java;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
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

public class BinaryJsonDocumentTest {

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
    final JsonObject content = JsonObject.empty().put("hello", "world");
    final JsonDocument doc = JsonDocument.create("insert", content);
    final JsonDocument response = bucket
        .insert(doc)
        .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
          @Override
          public Observable<JsonDocument> call(final JsonDocument document) {
            return bucket.get("insert");
          }
        })
        .toBlocking()
        .single();

    assertEquals(content.getString("hello"), response.content().getString("hello"));
  }

  @Test
  public void shouldUpsertAndGet() {
    final JsonObject content = JsonObject.empty().put("hello", "world");
    final JsonDocument doc = JsonDocument.create("upsert", content);
    final JsonDocument response = bucket
        .upsert(doc)
        .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
          @Override
          public Observable<JsonDocument> call(final JsonDocument document) {
            return bucket.get("upsert");
          }
        })
        .toBlocking()
        .single();

    assertEquals(content.getString("hello"), response.content().getString("hello"));
    assertEquals(ResponseStatus.SUCCESS, response.status());
  }

  @Test
  public void shouldUpsertAndReplace() {
    final JsonObject content = JsonObject.empty().put("hello", "world");
    final JsonDocument doc = JsonDocument.create("upsert-r", content);

    final JsonDocument response = bucket
        .upsert(doc)
        .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
          @Override
          public Observable<JsonDocument> call(final JsonDocument document) {
            return bucket.get("upsert-r");
          }
        })
        .toBlocking()
        .single();

    assertEquals(content.getString("hello"), response.content().getString("hello"));

    final JsonDocument updated = JsonDocument.from(response, JsonObject.empty().put("hello", "replaced"));
    final JsonDocument replacedResponse = bucket
        .replace(updated)
        .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
          @Override
          public Observable<JsonDocument> call(final JsonDocument document) {
            return bucket.get("upsert-r");
          }
        })
        .toBlocking()
        .single();

    assertEquals("replaced", replacedResponse.content().getString("hello"));
  }

  @Test
  public void shouldLoadMultipleDocuments() throws Exception {
    final BlockingObservable<JsonDocument> observable = Observable
        .from("doc1", "doc2", "doc3")
        .flatMap(new Func1<String, Observable<JsonDocument>>() {
          @Override
          public Observable<JsonDocument> call(final String id) {
            return bucket.get(id);
          }
        })
        .toBlocking();

    final Iterator<JsonDocument> iterator = observable.getIterator();
    while (iterator.hasNext()) {
      final JsonDocument doc = iterator.next();

      assertNull(doc.content());
      assertEquals(ResponseStatus.NOT_EXISTS, doc.status());
    }
  }

}
