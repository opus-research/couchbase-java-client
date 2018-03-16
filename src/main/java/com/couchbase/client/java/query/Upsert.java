package com.couchbase.client.java.query;

import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.Element;
import com.couchbase.client.java.query.dsl.path.AbstractPath;
import com.couchbase.client.java.query.dsl.path.DefaultInitialInsertPath;
import com.couchbase.client.java.query.dsl.path.InitialInsertPath;

import static com.couchbase.client.java.query.dsl.Expression.x;

public class Upsert {

  private Upsert() {}

  public static InitialInsertPath upsertInto(String bucket) {
    return new DefaultInitialInsertPath(new UpsertPath(x(bucket)));
  }

  public static InitialInsertPath upsertInto(Expression bucket) {
    return new DefaultInitialInsertPath(new UpsertPath(bucket));
  }

  public static InitialInsertPath upsertIntoCurrentBucket() {
    return new DefaultInitialInsertPath(new UpsertPath(x(CouchbaseAsyncBucket.CURRENT_BUCKET_IDENTIFIER)));
  }

  private static class UpsertPath extends AbstractPath {
    public UpsertPath(final Expression bucket) {
      super(null);
      element(new Element() {
        @Override
        public String export() {
          return "UPSERT INTO " + bucket.toString();
        }
      });
    }
  }

}
