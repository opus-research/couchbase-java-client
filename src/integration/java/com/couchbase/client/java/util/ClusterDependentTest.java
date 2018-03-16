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
package com.couchbase.client.java.util;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CouchbaseAsyncCluster;

import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.AsyncClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base test class for tests that need a working cluster reference.
 *
 * @author Michael Nitschinger
 */
public class ClusterDependentTest {

    private static final String seedNode = TestProperties.seedNode();
    private static final String bucketName = TestProperties.bucket();
    private static final String password = TestProperties.password();
    private static final String adminName = TestProperties.adminName();
    private static final String adminPassword = TestProperties.adminPassword();

    private static AsyncCluster cluster;
    private static AsyncBucket bucket;
    private static AsyncClusterManager clusterManager;

    @BeforeClass
    public static void connect() {
        cluster = CouchbaseAsyncCluster.create(seedNode);
        clusterManager = cluster.clusterManager(adminName, adminPassword).toBlocking().single();
        boolean exists = clusterManager.hasBucket(bucketName).toBlocking().single();
        if (!exists) {
            clusterManager.insertBucket(DefaultBucketSettings
                .builder()
                .name("default")
                .quota(256)
                .password("")
                .enableFlush(true)
                .type(BucketType.COUCHBASE)
                .build()).toBlocking().single();
        }

        bucket = cluster.openBucket(bucketName, password).toBlocking().single();
        bucket.bucketManager().toBlocking().single().flush().toBlocking().single();
    }

    @AfterClass
    public static void disconnect() throws InterruptedException {
        cluster.disconnect().toBlocking().single();
    }

    public static String password() {
        return password;
    }

    public static AsyncCluster cluster() {
        return cluster;
    }

    public static AsyncBucket bucket() {
        return bucket;
    }

    public static String bucketName() {
        return bucketName;
    }

    public static AsyncClusterManager clusterManager() {
        return clusterManager;
    }
}