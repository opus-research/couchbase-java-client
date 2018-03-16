/*
 * Copyright (C) 2016 Couchbase, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.InvalidPasswordException;
import com.couchbase.client.java.util.CouchbaseTestContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Simon Baslé
 * @since 2.2
 */
public class CredentialsAuthenticationTest  {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CredentialsAuthenticationTest.class);

    private static CouchbaseTestContext ctx;

    @BeforeClass
    public static void connect() throws Exception {
        ctx = CouchbaseTestContext.builder()
                .withEnv(DefaultCouchbaseEnvironment.builder().connectTimeout(10000))
                .bucketName("creds")
                .bucketPassword("protected")
                .adhoc(true)
                .bucketQuota(100)
                .bucketReplicas(1)
                .bucketType(BucketType.COUCHBASE)
                .build();
    }

    @AfterClass
    public static void disconnect() throws InterruptedException {
        ctx.destroyBucketAndDisconnect();
    }

    @Test
    public void testClusterManagerCredentialsInitialized() {
        //the fact that the context opens a ClusterManager initializes credentials
        String[][] creds = ctx.cluster().credentialsManager().getCredentials(AuthenticationContext.CLUSTER_MANAGEMENT, null);
        assertNotNull(creds);
        assertNotNull(creds[0]);
        assertEquals(ctx.adminName(), creds[0][0]);
        assertEquals(ctx.adminPassword(), creds[0][1]);
    }

    @Test
    public void testClusterManagerAutoOpening() {
        ClusterManager manager = ctx.cluster().clusterManager();

        assertNotSame(ctx.clusterManager(), manager);
        assertEquals(true, manager.hasBucket(ctx.bucketName()));
    }

    @Test
    public void testBucketOpeningWithoutExplicitPassword() {
        String name = "testBucketOpeningWithoutExplicitPassword";
        ctx.clusterManager().insertBucket(
                DefaultBucketSettings.builder()
                    .name(name)
                    .password("protected")
                    .quota(100)
                    .build());

        try {
            //check that without pre-existing credentials opening fails
            try {
                ctx.cluster().openBucket(name);
            } catch (CouchbaseException e) {
                //success
                LOGGER.warn("", e);
            }

            //add the credential
            ctx.cluster().credentialsManager().addBucketCredential(name, "protected");

            //check that with the credential it works
            Bucket bucket = ctx.cluster().openBucket(name);
            bucket.upsert(JsonDocument.create("toto"));
            assertNotNull(bucket);
        } finally {
            ctx.clusterManager().removeBucket(name);
        }
    }
}
