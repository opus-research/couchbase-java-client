package com.couchbase.client.java;

import com.couchbase.client.java.analytics.AnalyticsParams;
import com.couchbase.client.java.analytics.AnalyticsQuery;
import com.couchbase.client.java.analytics.AsyncAnalyticsQueryResult;
import com.couchbase.client.java.analytics.AsyncAnalyticsQueryRow;
import org.junit.Test;

import java.util.List;

/**
 * Created by daschl on 10/02/17.
 */
public class AnalyticsTest {

    @Test
    public void foo() throws Exception {

        System.setProperty("com.couchbase.analyticsEnabled", "true");

        Cluster cluster = CouchbaseCluster.create();
        CouchbaseAsyncBucket bucket = (CouchbaseAsyncBucket) cluster.openBucket().async();

        AsyncAnalyticsQueryResult single = bucket.query(AnalyticsQuery.simple("SELECT 1=1")).toBlocking().single();
        System.out.println(single);

        List<AsyncAnalyticsQueryRow> single1 = single.rows().toList().toBlocking().single();
        System.out.println(single1);

        System.out.println(single.info().toBlocking().single());
        System.out.println(single.clientContextId());

        Thread.sleep(100000);


    }
}
