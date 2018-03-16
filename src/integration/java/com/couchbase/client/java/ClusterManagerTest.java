package com.couchbase.client.java;

import com.couchbase.client.java.cluster.ClusterBucketSettings;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultClusterBucketSettings;
import com.couchbase.client.java.util.ClusterDependentTest;
import org.junit.Test;
import rx.functions.Action1;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ClusterManagerTest extends ClusterDependentTest {

    @Test
    public void shouldLoadInfo() {
        ClusterInfo info = clusterManager().info().toBlocking().single();

        assertNotNull(info);
        assertTrue(info.raw().getObject("storageTotals").getObject("ram").getLong("total") > 0);
    }

    @Test
    public void shouldGetBuckets() {
        clusterManager().getBuckets().toBlocking().forEach(new Action1<ClusterBucketSettings>() {
            @Override
            public void call(ClusterBucketSettings clusterBucketSettings) {
                System.err.println(clusterBucketSettings);
            }
        });
    }

    @Test
    public void shouldRemoveBucket() {
        // TODO: insert bucket first and then delete
        Boolean done = clusterManager().removeBucket("foo").toBlocking().single();
        assertTrue(done);
        System.out.println(done);
    }

    @Test
    public void shouldInsertBucket() {
        ClusterBucketSettings settings = DefaultClusterBucketSettings
            .builder()
            .name("foobar")
            .quota(512000)
            .build();

        clusterManager().insertBucket(settings).toBlocking().single();
    }

    @Test
    public void shouldUpdateBucket() {

    }

}
