package com.couchbase.client.java;

import com.couchbase.client.java.util.ClusterDependentTest;
import org.junit.Test;


public class QueryTest extends ClusterDependentTest {
    @Test
    public void shouldQueryView() throws Exception {
        System.out.println(bucket().query("select * from default limit 5").toList().toBlocking().single());
    }
}
