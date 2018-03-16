package com.couchbase.client.java.query;


import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.Element;
import com.couchbase.client.java.query.dsl.path.AbstractPath;
import com.couchbase.client.java.query.dsl.path.DefaultMergeSourcePath;
import com.couchbase.client.java.query.dsl.path.MergeSourcePath;

import static com.couchbase.client.java.query.dsl.Expression.x;

public class Merge {

  private Merge() {}


  public static MergeSourcePath mergeInto(String bucket) {
    return new DefaultMergeSourcePath(new MergePath(x(bucket)));
  }

  public static MergeSourcePath mergeInto(Expression bucket) {
    return new DefaultMergeSourcePath(new MergePath(bucket));
  }

  public static MergeSourcePath mergeIntoCurrentBucket() {
    return new DefaultMergeSourcePath(new MergePath(x(CouchbaseAsyncBucket.CURRENT_BUCKET_IDENTIFIER)));
  }

  private static class MergePath extends AbstractPath {
    public MergePath(final Expression bucket) {
      super(null);
      element(new Element() {
        @Override
        public String export() {
          return "MERGE INTO " + bucket.toString();
        }
      });
    }
  }
}
