/*
 * Copyright (C) 2015 Couchbase, Inc.
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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.repository.Repository;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import com.couchbase.client.java.util.features.Version;
import org.junit.Assume;

/**
 * Short description of class
 *
 * @author Simon Baslé
 * @since X.X
 */
public class CouchbaseTestContext {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    private final Bucket bucket;
    private final String bucketPassword;
    private final BucketManager bucketManager;
    private final Cluster cluster;
    private final ClusterManager clusterManager;
    private final String seedNode;
    private final String adminName;
    private final String adminPassword;
    private final CouchbaseEnvironment env;
    private final String bucketName;
    private final boolean isAdHoc;
    private final boolean isFlushEnabled;
    private final Repository repository;

    private CouchbaseTestContext(Bucket bucket, String bucketPassword,
            BucketManager bucketManager, Cluster cluster, ClusterManager clusterManager, String seedNode,
            String adminName, String adminPassword, CouchbaseEnvironment env, boolean isAdHoc, boolean isFlushEnabled) {
        this.bucket = bucket;
        this.bucketName = bucket.name();
        this.bucketPassword = bucketPassword;
        this.bucketManager = bucketManager;
        this.cluster = cluster;
        this.clusterManager = clusterManager;
        this.seedNode = seedNode;
        this.adminName = adminName;
        this.adminPassword = adminPassword;
        this.env = env;
        this.isAdHoc = isAdHoc;
        this.isFlushEnabled = isFlushEnabled;
        this.repository = bucket.repository();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private boolean createAdhocBucket;
        private boolean forceQueryEnabled;
        private String seedNode;
        private String adminName;
        private String adminPassword;
        private DefaultCouchbaseEnvironment.Builder envBuilder;
        private String bucketName;
        private String bucketPassword;
        private DefaultBucketSettings.Builder bucketSettingsBuilder;
        private boolean flushOnInit;

        public Builder() {
            forceQueryEnabled = TestProperties.queryEnabled();
            seedNode = TestProperties.seedNode();
            adminName = TestProperties.adminName();
            adminPassword = TestProperties.adminPassword();
            envBuilder = DefaultCouchbaseEnvironment.builder();
            if (forceQueryEnabled) {
                envBuilder.queryEnabled(true);
            }
            bucketName = TestProperties.bucket();
            bucketPassword = TestProperties.password();
            bucketSettingsBuilder = DefaultBucketSettings
                .builder()
                .quota(256)
                .enableFlush(true)
                .type(BucketType.COUCHBASE);
            flushOnInit = true;
            this.createAdhocBucket = false;
        }

        public Builder adhoc(boolean isAdhoc) {
            this.createAdhocBucket = isAdhoc;
            this.flushOnInit = false;
            return this;
        }

        public Builder forceQueryEnabled(boolean force) {
            this.forceQueryEnabled = force;
            return this;
        }

        public Builder seedNode(String seedNode) {
        this.seedNode = seedNode;
        return this;
        }

        public Builder adminName(String adminName) {
        this.adminName = adminName;
        return this;
        }

        public Builder adminPassword(String adminPassword) {
        this.adminPassword = adminPassword;
        return this;
        }

        public Builder withEnv(DefaultCouchbaseEnvironment.Builder envBuilder) {
        this.envBuilder = envBuilder;
        return this;
        }

        public Builder bucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
        }

        public Builder bucketPassword(String bucketPassword) {
        this.bucketPassword = bucketPassword;
        return this;
        }

        public Builder bucketQuota(int quota) {
            this.bucketSettingsBuilder.quota(quota);
            return this;
        }

        public Builder bucketType(BucketType type) {
            this.bucketSettingsBuilder.type(type);
            return this;
        }

        public Builder flushOnInit(boolean flushOnInit) {
            this.flushOnInit = flushOnInit;
            return this;
        }

        public CouchbaseTestContext build() {
            if (createAdhocBucket) {
                this.bucketName = "adHoc_" + this.bucketName + System.nanoTime();
            }
            
            this.bucketSettingsBuilder = bucketSettingsBuilder.name(this.bucketName)
                    .password(this.bucketPassword);
            this.envBuilder = envBuilder.queryEnabled(forceQueryEnabled);
            
            CouchbaseEnvironment env = envBuilder.build();

            Cluster cluster = CouchbaseCluster.create(env, seedNode);
            ClusterManager clusterManager = cluster.clusterManager(adminName, adminPassword);

            boolean existing = clusterManager.hasBucket(bucketName);
            if (!existing) {
                clusterManager.insertBucket(bucketSettingsBuilder.build());
            }

            boolean isFlushEnabled = bucketSettingsBuilder.enableFlush();

            Bucket bucket = cluster.openBucket(bucketName, bucketPassword);
            BucketManager bucketManager = bucket.bucketManager();

            if (flushOnInit && isFlushEnabled && existing) {
                bucketManager.flush();
            }

            return new CouchbaseTestContext(bucket, bucketPassword, bucketManager, cluster, clusterManager, seedNode,
                    adminName, adminPassword, env, createAdhocBucket, isFlushEnabled);
        }

    }

    //==========================
    //== Lifecycle Management ==
    //==========================

    public void flush() {
        if (isFlushEnabled) {
            bucketManager.flush();
        }
    }

    public void deleteAll() {
        //test for N1QL
        if (env.queryEnabled() || clusterManager.info().checkAvailable(CouchbaseFeature.N1QL)) {
            N1qlQueryResult result = bucket.query(N1qlQuery.simple("DELETE FROM `" + bucketName + "`"));
            if (!result.finalSuccess()) {
                throw new CouchbaseException("Could not DELETE ALL - " + result.errors().toString());
            }
        }
    }

    public void destroyBucketAndDisconnect() {
        clusterManager.removeBucket(bucketName);
        disconnect();
    }

    public void disconnect() {
        cluster.disconnect();
    }

    //=====================
    //== Utility Methods ==
    //=====================

    public CouchbaseTestContext ignoreIfNoN1ql() {
        return ignoreIfMissing(CouchbaseFeature.N1QL, env.queryEnabled());
    }

    /**
     * By calling this in @BeforeClass with a {@link CouchbaseFeature},
     * tests will be skipped is said feature is not available on the cluster.
     *
     * @param feature the feature to check for.
     */
    public CouchbaseTestContext ignoreIfMissing(CouchbaseFeature feature) {
        return ignoreIfMissing(feature, false);
    }

    /**
     * By calling this in @BeforeClass with a {@link CouchbaseFeature},
     * tests will be skipped is said feature is not available on the cluster, unless forced is set to true.
     *
     * @param feature the feature to check for.
     * @param forced if true, always consider the feature available.
     */
    public CouchbaseTestContext ignoreIfMissing(CouchbaseFeature feature, boolean forced) {
        Assume.assumeTrue(forced || clusterManager.info().checkAvailable(feature));
        return this;
    }

    /**
     * By calling this in @BeforeClass with a {@link Version},
     * tests will be skipped is all nodes in the cluster are not above
     * or at that version.
     *
     * @param minimumVersion the required version to check for.
     */
    public CouchbaseTestContext ignoreIfClusterUnder(Version minimumVersion) {
        Assume.assumeTrue(clusterManager().info().getMinVersion().compareTo(minimumVersion) >= 0);
        return this;
    }

    /**
     * Utility method to get a meaningful test fail message out of a {@link N1qlQueryResult}'s {@link N1qlQueryResult#errors()} list.
     * @param message the prefix to the message.
     * @param queryResult the query result (null will be ignored).
     * @return the message with the list of N1QL errors appended to it.
     */
    public static String errorMsg(String message, N1qlQueryResult queryResult) {
        if (message == null) {
            return (queryResult == null) ? null : queryResult.errors().toString();
        }

        if (queryResult == null) {
            return message;
        }

        return message + " - " + queryResult.errors().toString();
    }


    //=============
    //== Getters ==
    //=============

    public Bucket bucket() {
        return bucket;
    }

    public String bucketPassword() {
        return bucketPassword;
    }

    public BucketManager bucketManager() {
        return bucketManager;
    }

    public Repository repository() {
        return repository;
    }

    public Cluster cluster() {
        return cluster;
    }

    public ClusterManager clusterManager() {
        return clusterManager;
    }

    public String adminName() {
        return adminName;
    }

    public String adminPassword() {
        return adminPassword;
    }

    public CouchbaseEnvironment env() {
        return env;
    }

    public String bucketName() {
        return bucketName;
    }

    public boolean isAdHoc() {
        return isAdHoc;
    }

    public boolean isFlushEnabled() {
        return isFlushEnabled;
    }
}
