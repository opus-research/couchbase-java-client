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

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * An enum of the various contexts of authentication that can be stored in a {@link CredentialsManager}.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public enum AuthenticationContext {

    /**
     * Bucket-level credentials for Key/Value operations.
     */
    BUCKET_KEYVALUE("bucket-kv"),
    /**
     * Bucket-level credentials for N1QL querying on that single bucket.
     */
    BUCKET_N1QL("bucket-n1ql"),
    /**
     * Cluster-level credentials that can be used to perform N1QL queries on the whole cluster.
     */
    CLUSTER_N1QL("cluster-n1ql"),
    /**
     * Cluster-level credentials to perform FTS queris on the whole cluster.
     */
    CLUSTER_FTS("cluster-cbft"),
    /**
     * Cluster-level credentials used for cluster management operations.
     */
    CLUSTER_MANAGEMENT("cluster-mgmt");

    private final String sdkCompatibleRepresentation;

    AuthenticationContext(String sdkCompatibleRepresentation) {
        this.sdkCompatibleRepresentation = sdkCompatibleRepresentation;
    }

    public String getSdkCompatibleRepresentation() {
        return sdkCompatibleRepresentation;
    }
}
