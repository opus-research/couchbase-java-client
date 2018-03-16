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
package com.couchbase.client.java;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.java.cluster.AsyncClusterManager;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.transcoder.Transcoder;
import rx.Observable;

import java.util.List;

/**
 * Represents a Couchbase Server {@link AsyncCluster}.
 *
 * A {@link AsyncCluster} is able to open many {@link AsyncBucket}s while sharing the underlying resources (like sockets)
 * very efficiently. In addition, a {@link AsyncClusterManager} is available to perform cluster-wide operations.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public interface AsyncCluster {

    /**
     * Open the default {@link AsyncBucket}.
     *
     * @return a {@link Observable} containing the {@link AsyncBucket} reference once opened.
     */
    Observable<AsyncBucket> openBucket();

    /**
     * Open the given {@link AsyncBucket} without a password (if not set during creation).
     *
     * @param name the name of the bucket.
     * @return a {@link Observable} containing the {@link AsyncBucket} reference once opened.
     */
    Observable<AsyncBucket> openBucket(String name);

    /**
     * Open the given {@link AsyncBucket} with a password (set during creation).
     *
     * @param name the name of the bucket.
     * @param password the password of the bucket, can be an empty string.
     * @return a {@link Observable} containing the {@link AsyncBucket} reference once opened.
     */
    Observable<AsyncBucket> openBucket(String name, String password);

    /**
     * Open the given {@link AsyncBucket} with a password and a custom list of transcoders.
     *
     * @param name the name of the bucket.
     * @param password the password of the bucket, can be an empty string.
     * @param transcoders a list of custom transcoders.
     * @return a {@link Observable} containing the {@link AsyncBucket} reference once opened.
     */
    Observable<AsyncBucket> openBucket(String name, String password, List<Transcoder<? extends Document, ?>> transcoders);

    /**
     * Returns a reference to the {@link AsyncClusterManager}.
     *
     * The {@link AsyncClusterManager} allows to perform cluster level management operations. It requires administrative
     * credentials, which have been set during cluster configuration. Bucket level credentials are not enough to perform
     * cluster-level operations.
     *
     * @param username privileged username.
     * @param password privileged password.
     * @return a {@link Observable} containing the {@link AsyncClusterManager}.
     */
    Observable<AsyncClusterManager> clusterManager(String username, String password);

    /**
     * Disconnects from the {@link AsyncCluster} and closes all open {@link AsyncBucket}s.
     *
     * @return a {@link Observable} containing true if successful and failing the {@link Observable} otherwise.
     */
    Observable<Boolean> disconnect();

    /**
     * Returns a reference to the underlying core engine.
     *
     * Since the {@link ClusterFacade} provides direct access to low-level semantics, no sanity checks are performed as
     * with the Java SDK itself. Handle with care and only use it when absolutely needed.
     *
     * @return a {@link Observable} containing the core engine.
     */
    Observable<ClusterFacade> core();

}
