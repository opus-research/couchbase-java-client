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

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.message.internal.GetConfigProviderRequest;
import com.couchbase.client.core.message.internal.GetConfigProviderResponse;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.java.Bucket;
import rx.functions.Action1;
import rx.functions.Func1;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

/**
 * Helper class to provide direct access on how document IDs are mapped onto nodes.
 *
 * @author Michael Nitschinger
 * @since 2.1.0
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Public
public class DocumentIdLocator {

    private final ConfigurationProvider configProvider;
    private final AtomicReference<BucketConfig> bucketConfig;

    private DocumentIdLocator(final Bucket bucket) {
        configProvider = bucket
            .core()
            .<GetConfigProviderResponse>send(new GetConfigProviderRequest())
            .toBlocking()
            .single()
            .provider();

        bucketConfig = new AtomicReference<BucketConfig>(configProvider.config().bucketConfig(bucket.name()));

        configProvider
            .configs()
            .filter(new Func1<ClusterConfig, Boolean>() {
                @Override
                public Boolean call(ClusterConfig clusterConfig) {
                    return clusterConfig.hasBucket(bucket.name());
                }
            }).subscribe(new Action1<ClusterConfig>() {
                @Override
                public void call(ClusterConfig config) {
                    bucketConfig.set(config.bucketConfig(bucket.name()));
                }
            });
    }

    /**
     * Creates a new {@link DocumentIdLocator}, mapped on to the given {@link Bucket}.
     *
     * @param bucket the scoped bucket.
     * @return the created locator.
     */
    public static DocumentIdLocator create(final Bucket bucket) {
        return new DocumentIdLocator(bucket);
    }

    /**
     * Returns the target active node {@link InetAddress} for a given document ID on the bucket.
     *
     * @param id the document id to convert.
     * @return the node for the given document id.
     */
    public InetAddress activeNodeForId(final String id) {
        BucketConfig config = bucketConfig.get();

        if (config instanceof CouchbaseBucketConfig) {
            return nodeForIdOnCouchbaseBucket(id, (CouchbaseBucketConfig) config);
        } else if (config instanceof MemcachedBucketConfig) {
            return nodeForIdOnMemcachedBucket(id, (MemcachedBucketConfig) config);
        } else {
            throw new UnsupportedOperationException("Bucket type not supported: " + config.getClass().getName());
        }
    }

    /**
     * Returns all target replica nodes {@link InetAddress} for a given document ID on the bucket.
     *
     * @param id the document id to convert.
     * @return the node for the given document id.
     */
    public List<InetAddress> replicaNodesForId(final String id) {
        BucketConfig config = bucketConfig.get();

        if (config instanceof CouchbaseBucketConfig) {
            CouchbaseBucketConfig cbc = (CouchbaseBucketConfig) config;
            List<InetAddress> replicas = new ArrayList<InetAddress>();
            for (int i = 1; i <= cbc.numberOfReplicas(); i++) {
                replicas.add(replicaNodeForId(id, i));
            }
            return replicas;
        } else {
            throw new UnsupportedOperationException("Bucket type not supported: " + config.getClass().getName());
        }
    }

    /**
     * Returns the target replica node {@link InetAddress} for a given document ID and replica number on the bucket.
     *
     * @param id the document id to convert.
     * @param replicaNum the replica number.
     * @return the node for the given document id.
     */
    public InetAddress replicaNodeForId(final String id, int replicaNum) {
        BucketConfig config = bucketConfig.get();

        if (config instanceof CouchbaseBucketConfig) {
            CouchbaseBucketConfig cbc = (CouchbaseBucketConfig) config;
            int partitionId = (int) hashId(id) & cbc.numberOfPartitions() - 1;
            int nodeId = cbc.nodeIndexForReplica(partitionId, replicaNum - 1);
            if (nodeId == -1) {
                throw new IllegalStateException("No partition assigned to node for Document ID: " + id);
            }
            if (nodeId == -2) {
                throw new IllegalStateException("Replica not configured for this bucket.");
            }
            return cbc.nodeAtIndex(nodeId).hostname();
        }  else {
            throw new UnsupportedOperationException("Bucket type not supported: " + config.getClass().getName());
        }
    }

    private static InetAddress nodeForIdOnCouchbaseBucket(final String id, final CouchbaseBucketConfig config) {
        int partitionId = (int) hashId(id) & config.numberOfPartitions() - 1;
        int nodeId = config.nodeIndexForMaster(partitionId);
        if (nodeId == -1) {
            throw new IllegalStateException("No partition assigned to node for Document ID: " + id);
        }
        return config.nodeAtIndex(nodeId).hostname();
    }

    private static InetAddress nodeForIdOnMemcachedBucket(final String id, final MemcachedBucketConfig config) {
        long hash = ketamaHash(id);
        if (!config.ketamaNodes().containsKey(hash)) {
            SortedMap<Long, NodeInfo> tailMap = config.ketamaNodes().tailMap(hash);
            if (tailMap.isEmpty()) {
                hash = config.ketamaNodes().firstKey();
            } else {
                hash = tailMap.firstKey();
            }
        }
        return config.ketamaNodes().get(hash).hostname();
    }

    private static long hashId(String id) {
        CRC32 crc32 = new CRC32();
        try {
            crc32.update(id.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return (crc32.getValue() >> 16) & 0x7fff;
    }

    private static long ketamaHash(final String key) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(key.getBytes(CharsetUtil.UTF_8));
            byte[] digest = md5.digest();
            long rv = ((long) (digest[3] & 0xFF) << 24)
                | ((long) (digest[2] & 0xFF) << 16)
                | ((long) (digest[1] & 0xFF) << 8)
                | (digest[0] & 0xFF);
            return rv & 0xffffffffL;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Could not encode ketama hash.", e);
        }
    }

}
