/**
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.util;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import rx.Observable;
import rx.Single;
import rx.functions.Func1;

import java.util.concurrent.TimeUnit;

/**
 * The {@link TransparentReplicaGetHelper} abstracts common logic to first grab the
 * active document and if that fails tries all available replicas and returns the first
 * result.
 *
 * NOTE: Using these APIs is eventually consistent meaning that you cannot rely on
 * a previous successful mutation to a document be reflected in the result. Use this
 * API only if you favor availability over consistency on the read path.
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class TransparentReplicaGetHelper {

    private final Bucket bucket;
    private final long primaryTimeout;
    private final long replicaTimeout;

    private TransparentReplicaGetHelper(final Bucket bucket, final long primaryTimeout,
        final long replicaTimeout) {
        this.bucket = bucket;
        this.primaryTimeout = primaryTimeout;
        this.replicaTimeout = replicaTimeout;
    }

    /**
     * Creates a new {@link TransparentReplicaGetHelper} with the default KV timeout configured
     * on the bucket environment.
     *
     * @param bucket the bucket instance used to fetch the documents from.
     * @return the usable {@link TransparentReplicaGetHelper}.
     */
    public static TransparentReplicaGetHelper create(final Bucket bucket) {
        return create(bucket, bucket.environment().kvTimeout());
    }

    /**
     * Creates a new {@link TransparentReplicaGetHelper} with a custom timeout used for both
     * the primary and replica get operations.
     *
     * @param bucket the bucket instance used to fetch the documents from.
     * @param timeout the timeout in MS used for both primary and replica operations.
     * @return the usable {@link TransparentReplicaGetHelper}.
     */
    public static TransparentReplicaGetHelper create(final Bucket bucket, final long timeout) {
        return create(bucket, timeout, timeout);
    }

    /**
     * Creates a new {@link TransparentReplicaGetHelper} with different custom timeouts for both the
     * primary and replica get operations.
     *
     * @param bucket the bucket instance used to fetch the documents from.
     * @param primaryTimeout the timeout in MS used for the primary get.
     * @param replicaTimeout the timeout in MS used for the replica get.
     * @return the usable {@link TransparentReplicaGetHelper}.
     */
    public static TransparentReplicaGetHelper create(final Bucket bucket, final long primaryTimeout,
        final long replicaTimeout) {
        return new TransparentReplicaGetHelper(bucket, primaryTimeout, replicaTimeout);
    }

    /**
     * Asynchronously fetch the document from the primary and if that operations fails try
     * all the replicas and return the first document that comes back from them.
     *
     * @param id the document ID to fetch.
     * @return an {@link Observable} with either 0 or 1 {@link JsonDocument}.
     */
    public Single<JsonDocument> getFirstPrimaryOrReplica(final String id) {
        return getFirstPrimaryOrReplica(id, JsonDocument.class);
    }

    /**
     * Asynchronously fetch the document from the primary and if that operations fails try
     * all the replicas and return the first document that comes back from them.
     *
     * @param id the document ID to fetch.
     * @param target the custom document type to use.
     * @return @return an {@link Observable} with either 0 or 1 {@link Document}.
     */
    public <D extends Document<?>> Single<D> getFirstPrimaryOrReplica(final String id, final Class<D> target) {
        Observable<D> fallback = bucket
            .async()
            .getFromReplica(id, ReplicaMode.ALL, target)
            .timeout(replicaTimeout, TimeUnit.MILLISECONDS)
            .firstOrDefault(null)
            .filter(new Func1<D, Boolean>() {
                @Override
                public Boolean call(D d) {
                    return d != null;
                }
            });

        return bucket
            .async()
            .get(id, target)
            .timeout(primaryTimeout, TimeUnit.MILLISECONDS)
            .onErrorResumeNext(fallback)
            .toSingle();
    }
}
