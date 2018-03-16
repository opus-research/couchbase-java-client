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

import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.repository.Repository;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import com.couchbase.client.java.util.features.Version;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

/**
 * An helper class for integration tests that defaults to values from {@link TestProperties}
 * but can be overridden on a case by case basis. Use the {@link #builder()} to initialize
 * the context in a JUnit {@link BeforeClass} annotated method, then get the SDK components
 * you need for your tests from this context (eg. {@link #bucket()}).
 *
 * You can have the test context create an adhoc bucket for you ({@link Builder#adhoc(boolean)},
 * in which case you should set a low quota ({@link Builder#bucketQuota(int)} of 100) and call
 * {@link #destroyBucketAndDisconnect()} in a {@link AfterClass} annotated method.
 *
 * @author Simon Baslé
 * @since 2.2
 */
public class CouchbaseTestContext {

    public static final String AD_HOC = "adHoc_";

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

    /**
     * @return a {@link Builder} for a new {@link CouchbaseTestContext}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * If N1QL is available (detected or forced), this method will attempt to create a PRIMARY INDEX on the bucket.
     * It will ignore an already existing primary index. If other N1QL errors arise, a {@link CouchbaseException} will
     * be thrown (with the message containing the list of errors).
     */
    public CouchbaseTestContext ensurePrimaryIndex() {
        //test for N1QL
        if (env.queryEnabled() || clusterManager.info().checkAvailable(CouchbaseFeature.N1QL)) {
            N1qlQueryResult result = bucket().query(
                    N1qlQuery.simple("CREATE PRIMARY INDEX ON `" + bucketName() + "`",
                            N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)), 2, TimeUnit.MINUTES);
            if (!result.finalSuccess()) {
                //ignore "index already exist"
                for (JsonObject error : result.errors()) {
                    if (!error.getString("msg").contains("already exist")) {
                        throw new CouchbaseException("Could not CREATE PRIMARY INDEX - " + result.errors().toString());
                    }
                }
            }
        }
        return this;
    }

    /**
     * Builder for a {@link CouchbaseTestContext} that allows you to set all the options for
     * creating a tailored integration test environment. Default values will be taken from {@link TestProperties}
     * and, if it doesn't exist, the requested bucket will be created as a {@link BucketType#COUCHBASE} with a
     * {@link #bucketType(BucketType) bucket} with {@link #bucketQuota(int) memory quota} of 256MB and
     * {@link #flushOnInit(boolean) flush} enabled.
     */
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

        /**
         * Set adhoc to true to force creation of a bucket for the duration of the test case (the name will be
         * prefixed by "{@value CouchbaseTestContext#AD_HOC}" and suffixed with a random number). The bucket won't be flushed as it is brand new.
         *
         * Don't forget to clean it up at the end, eg. using {@link CouchbaseTestContext#destroyBucketAndDisconnect()}.
         */
        public Builder adhoc(boolean isAdhoc) {
            this.createAdhocBucket = isAdhoc;
            this.flushOnInit = false;
            return this;
        }

        /**
         * Forces the environment to be created with {@link CouchbaseEnvironment#queryEnabled()} set to true.
         */
        public Builder forceQueryEnabled(boolean force) {
            this.forceQueryEnabled = force;
            return this;
        }

        /**
         * Changes the seed node used for {@link Cluster} creation.
         */
        public Builder seedNode(String seedNode) {
            this.seedNode = seedNode;
            return this;
        }

        /**
         * Changes the administrator name used for {@link Cluster} and {@link ClusterManager} creation.
         */
        public Builder adminName(String adminName) {
            this.adminName = adminName;
            return this;
        }

        /**
         * Changes the administrator password used for {@link Cluster} and {@link ClusterManager} creation.
         */
        public Builder adminPassword(String adminPassword) {
            this.adminPassword = adminPassword;
            return this;
        }

        /**
         * Forces an environment configuration to be used. Note that the builder is required, and its configuration
         * will be changed if {@link #forceQueryEnabled(boolean)} was used.
         */
        public Builder withEnv(DefaultCouchbaseEnvironment.Builder envBuilder) {
            this.envBuilder = envBuilder;
            return this;
        }

        /**
         * Changes the bucket name that will be provided by this context. Note that the name could vary if
         * {@link #adhoc(boolean)} is true.
         */
        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        /**
         * Changes the bucket password used in opening the context's bucket.
         */
        public Builder bucketPassword(String bucketPassword) {
            this.bucketPassword = bucketPassword;
            return this;
        }

        /**
         * Changes the bucket RAM quota used if the bucket needs to be created (it doesn't exist or adhoc was used).
         */
        public Builder bucketQuota(int quota) {
            this.bucketSettingsBuilder.quota(quota);
            return this;
        }

        /**
         * Changes the bucket type used if the bucket needs to be created (it doesn't exist or adhoc was used).
         */
        public Builder bucketType(BucketType type) {
            this.bucketSettingsBuilder.type(type);
            return this;
        }

        /**
         * Changes the configured number of replicas if the bucket needs to be created (it doesn't exist or adhoc was used).
         */
        public Builder bucketReplicas(int replicas) {
            this.bucketSettingsBuilder.replicas(replicas);
            return this;
        }

        /**
         * Set to true to activate a flush upon building the context, unless the bucket was not previously existing
         * or flush is disabled on the bucket.
         */
        public Builder flushOnInit(boolean flushOnInit) {
            this.flushOnInit = flushOnInit;
            return this;
        }

        /**
         * Build the {@link CouchbaseTestContext}, triggering potential creation of a bucket, flush of a bucket, etc...
         * (see {@link #adhoc(boolean)}, {@link #flushOnInit(boolean)}, ...).
         */
        public CouchbaseTestContext build() {
            if (createAdhocBucket) {
                this.bucketName = AD_HOC + this.bucketName + System.nanoTime();
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

    /**
     * Trigger a flush of the context's bucket.
     */
    public void flush() {
        if (isFlushEnabled) {
            bucketManager.flush();
        }
    }

    /**
     * If N1QL is available in this context, issue a DELETE ALL query.
     */
    public void deleteAll() {
        //test for N1QL
        if (env.queryEnabled() || clusterManager.info().checkAvailable(CouchbaseFeature.N1QL)) {
            N1qlQueryResult result = bucket.query(N1qlQuery.simple("DELETE FROM `" + bucketName + "`"));
            if (!result.finalSuccess()) {
                throw new CouchbaseException("Could not DELETE ALL - " + result.errors().toString());
            }
        }
    }

    /**
     * Remove the bucket (if it was adhoc) and disconnect from the cluster.
     */
    public void destroyBucketAndDisconnect() {
        if (isAdHoc) {
            clusterManager.removeBucket(bucketName);
        }
        disconnect();
    }

    /**
     * Disconnect from the cluster.
     */
    public void disconnect() {
        cluster.disconnect();
    }

    //=====================
    //== Utility Methods ==
    //=====================

    /**
     * By calling this in @BeforeClass, tests will be skipped if N1QL is unavailable and is not forced on the env.
     */
    public CouchbaseTestContext ignoreIfNoN1ql() {
        return ignoreIfMissing(CouchbaseFeature.N1QL, env.queryEnabled());
    }

    /**
     * By calling this in @BeforeClass with a {@link CouchbaseFeature},
     * tests will be skipped if said feature is not available on the cluster.
     *
     * @param feature the feature to check for.
     */
    public CouchbaseTestContext ignoreIfMissing(CouchbaseFeature feature) {
        return ignoreIfMissing(feature, false);
    }

    /**
     * By calling this in @BeforeClass with a {@link CouchbaseFeature},
     * tests will be skipped if said feature is not available on the cluster, unless forced is set to true.
     *
     * @param feature the feature to check for.
     * @param forced if true, always consider the feature available.
     */
    public CouchbaseTestContext ignoreIfMissing(CouchbaseFeature feature, boolean forced) {
        Assume.assumeTrue("Feature " + feature + " not available and not forced", forced || clusterManager.info().checkAvailable(feature));
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
        Assume.assumeTrue("Cluster is under " + minimumVersion, clusterManager().info().getMinVersion().compareTo(minimumVersion) >= 0);
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

    /** @return the {@link Bucket} to be used for tests in this context. */
    public Bucket bucket() {
        return bucket;
    }

    /** @return the password used to open the {@link #bucket()}. */
    public String bucketPassword() {
        return bucketPassword;
    }

    /** @return the {@link BucketManager} to be used for tests in this context. */
    public BucketManager bucketManager() {
        return bucketManager;
    }

    /** @return the {@link Repository} associated to the {@link #bucket()} used for tests in this context. */
    public Repository repository() {
        return repository;
    }

    /** @return the {@link Cluster} to be used for tests in this context. */
    public Cluster cluster() {
        return cluster;
    }

    /** @return the {@link ClusterManager} to be used for tests in this context. */
    public ClusterManager clusterManager() {
        return clusterManager;
    }

    /** @return the administrative login for the {@link #cluster()}. */
    public String adminName() {
        return adminName;
    }

    /** @return the administrative password for the {@link #cluster()}. */
    public String adminPassword() {
        return adminPassword;
    }

    /** @return the {@link CouchbaseEnvironment} to be used for tests in this context. */
    public CouchbaseEnvironment env() {
        return env;
    }

    /** @return the name of the {@link #bucket()} to be used for tests in this context. */
    public String bucketName() {
        return bucketName;
    }

    /**
     * Tells if the {@link #bucket()} to be used for tests in this context is ad hoc,
     * meaning that it was created for this specific context and can be destroyed at the end of the test.
     *
     * Adhoc buckets have the name initially configured in the builder prefixed with
     * "{@value CouchbaseTestContext#AD_HOC}" and suffixed with the system time in nanoseconds.
     * This makes up the name returned by {@link #bucketName()}.
     *
     * Note that such a bucket won't be flushed even if the instruction to flush was activated in the builder.
     *
     * @return true if the bucket is adhoc and can be destroyed after the tests, false otherwise.
     */
    public boolean isAdHoc() {
        return isAdHoc;
    }

    /** @return true if the {@link #bucket()} has flush capability enabled. */
    public boolean isFlushEnabled() {
        return isFlushEnabled;
    }
}
