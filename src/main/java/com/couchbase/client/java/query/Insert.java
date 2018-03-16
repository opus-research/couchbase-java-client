package com.couchbase.client.java.query;

import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.Element;
import com.couchbase.client.java.query.dsl.path.AbstractPath;
import com.couchbase.client.java.query.dsl.path.DefaultInitialInsertPath;
import com.couchbase.client.java.query.dsl.path.InitialInsertPath;

import static com.couchbase.client.java.query.dsl.Expression.x;

public class Insert {

  private Insert() {}

  public static InitialInsertPath insertInto(String bucket) {
    return new DefaultInitialInsertPath(new InsertPath(x(bucket)));
  }

  public static InitialInsertPath insertInto(Expression bucket) {
    return new DefaultInitialInsertPath(new InsertPath(bucket));
  }

  public static InitialInsertPath insertIntoCurrentBucket() {
    return new DefaultInitialInsertPath(new InsertPath(x(CouchbaseAsyncBucket.CURRENT_BUCKET_IDENTIFIER)));
  }

  private static class InsertPath extends AbstractPath {
    public InsertPath(final Expression bucket) {
      super(null);
      element(new Element() {
        @Override
        public String export() {
          return "INSERT INTO " + bucket.toString();
        }
      });
    }
  }

}
