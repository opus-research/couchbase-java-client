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

@InterfaceStability.Uncommitted
@InterfaceAudience.Public
public class DocumentIdLocator {

    private final ConfigurationProvider configProvider;
    private final AtomicReference<BucketConfig> bucketConfig;

    public DocumentIdLocator(final Bucket bucket) {
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
     * Returns the target {@link InetAddress} for a given document ID on the bucket.
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

    public InetAddress replicaNodeForId(final String id, int replicaNum) {
        BucketConfig config = bucketConfig.get();
        if (config instanceof CouchbaseBucketConfig) {
            CouchbaseBucketConfig cbc = (CouchbaseBucketConfig) config;

            CRC32 crc32 = new CRC32();
            try {
                crc32.update(id.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            long rv = (crc32.getValue() >> 16) & 0x7fff;
            int partitionId = (int) rv & cbc.numberOfPartitions() - 1;
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

    private InetAddress nodeForIdOnCouchbaseBucket(final String id, final CouchbaseBucketConfig config) {
        CRC32 crc32 = new CRC32();
        try {
            crc32.update(id.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
           throw new RuntimeException(e);
        }

        long rv = (crc32.getValue() >> 16) & 0x7fff;
        int partitionId = (int) rv & config.numberOfPartitions() - 1;
        int nodeId = config.nodeIndexForMaster(partitionId);

        if (nodeId == -1) {
            throw new IllegalStateException("No partition assigned to node for Document ID: " + id);
        }

        NodeInfo nodeInfo = config.nodeAtIndex(nodeId);
        return nodeInfo.hostname();
    }

    private InetAddress nodeForIdOnMemcachedBucket(final String id, final MemcachedBucketConfig config) {
        long hash = ketamaHash(id);
        if (!config.ketamaNodes().containsKey(hash)) {
            SortedMap<Long, NodeInfo> tailMap = config.ketamaNodes().tailMap(hash);
            if (tailMap.isEmpty()) {
                hash = config.ketamaNodes().firstKey();
            } else {
                hash = tailMap.firstKey();
            }
        }

        NodeInfo found = config.ketamaNodes().get(hash);
        return found.hostname();
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
