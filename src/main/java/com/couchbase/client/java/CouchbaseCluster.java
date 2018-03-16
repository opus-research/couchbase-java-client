package com.couchbase.client.java;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.java.cluster.AsyncClusterManager;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultClusterManager;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.transcoder.Transcoder;
import rx.functions.Func1;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CouchbaseCluster implements Cluster {

    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    private static final String DEFAULT_BUCKET = "default";
    private static final String DEFAULT_HOST = "127.0.0.1";
    private final CouchbaseAsyncCluster couchbaseAsyncCluster;
    private final CouchbaseEnvironment environment;
    private final ConnectionString connectionString;

    public static CouchbaseAsyncCluster create() {
        return create(DEFAULT_HOST);
    }

    public static CouchbaseAsyncCluster create(final CouchbaseEnvironment environment) {
        return create(environment, DEFAULT_HOST);
    }

    public static CouchbaseAsyncCluster create(final String... nodes) {
        return create(Arrays.asList(nodes));
    }

    public static CouchbaseAsyncCluster create(final List<String> nodes) {
        return new CouchbaseAsyncCluster(DefaultCouchbaseEnvironment.create(), ConnectionString.fromHostnames(nodes), false);
    }

    public static CouchbaseAsyncCluster create(final CouchbaseEnvironment environment, final String... nodes) {
        return create(environment, Arrays.asList(nodes));
    }

    public static CouchbaseAsyncCluster create(final CouchbaseEnvironment environment, final List<String> nodes) {
        return new CouchbaseAsyncCluster(environment, ConnectionString.fromHostnames(nodes), true);
    }

    public static CouchbaseAsyncCluster fromConnectionString(final String connectionString) {
        return new CouchbaseAsyncCluster(DefaultCouchbaseEnvironment.create(), ConnectionString.create(connectionString), false);
    }

    public static CouchbaseAsyncCluster fromConnectionString(final CouchbaseEnvironment environment, final String connectionString) {
        return new CouchbaseAsyncCluster(environment, ConnectionString.create(connectionString), true);
    }

    CouchbaseCluster(final CouchbaseEnvironment environment, final ConnectionString connectionString, final boolean sharedEnvironment) {
        couchbaseAsyncCluster = new CouchbaseAsyncCluster(environment, connectionString, sharedEnvironment);
        this.environment = environment;
        this.connectionString = connectionString;
    }

    @Override
    public Bucket openBucket() {
        return openBucket(DEFAULT_BUCKET);
    }

    @Override
    public Bucket openBucket(long timeout, TimeUnit timeUnit) {
        return openBucket(DEFAULT_BUCKET, timeout, timeUnit);
    }

    @Override
    public Bucket openBucket(String name) {
        return openBucket(name, null);
    }

    @Override
    public Bucket openBucket(String name, long timeout, TimeUnit timeUnit) {
        return openBucket(name, null, timeout, timeUnit);
    }

    @Override
    public Bucket openBucket(String name, String password) {
        return openBucket(name, password, null);
    }

    @Override
    public Bucket openBucket(String name, String password, long timeout, TimeUnit timeUnit) {
        return openBucket(name, password, null, timeout, timeUnit);
    }

    @Override
    public Bucket openBucket(String name, String password, List<Transcoder<? extends Document, ?>> transcoders) {
        return openBucket(name, password, transcoders, environment.connectTimeout(), TIMEOUT_UNIT);
    }

    @Override
    public Bucket openBucket(final String name, final String password, final List<Transcoder<? extends Document, ?>> transcoders, long timeout, TimeUnit timeUnit) {
        return couchbaseAsyncCluster
            .openBucket(name, password, transcoders)
            .map(new Func1<AsyncBucket, Bucket>() {
                @Override
                public Bucket call(AsyncBucket asyncBucket) {
                    return new CouchbaseBucket(environment, core(), name, password, transcoders);
                }
            })
            .timeout(timeout, timeUnit)
            .toBlocking()
            .single();
    }

    @Override
    public ClusterManager clusterManager(final String username, final String password) {
        return couchbaseAsyncCluster
            .clusterManager(username, password)
            .map(new Func1<AsyncClusterManager, ClusterManager>() {
                @Override
                public ClusterManager call(AsyncClusterManager asyncClusterManager) {
                    return DefaultClusterManager.create(username, password, connectionString, environment, core());
                }
            })
            .toBlocking()
            .single();
    }

    @Override
    public Boolean disconnect() {
        return disconnect(environment.disconnectTimeout(), TIMEOUT_UNIT);
    }

    @Override
    public Boolean disconnect(long timeout, TimeUnit timeUnit) {
        return couchbaseAsyncCluster
            .disconnect()
            .timeout(timeout, timeUnit)
            .toBlocking()
            .single();
    }

    @Override
    public ClusterFacade core() {
        return couchbaseAsyncCluster
            .core()
            .toBlocking()
            .single();
    }
}
