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

import static com.couchbase.client.java.AuthenticationContext.BUCKET_KEYVALUE;
import static com.couchbase.client.java.AuthenticationContext.BUCKET_N1QL;
import static com.couchbase.client.java.AuthenticationContext.CLUSTER_FTS;
import static com.couchbase.client.java.AuthenticationContext.CLUSTER_MANAGEMENT;
import static com.couchbase.client.java.AuthenticationContext.CLUSTER_N1QL;

import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;

/**
 * A class in charge of holding credentials for {@link Cluster}, {@link Bucket} and
 * queries...
 *
 * @author Simon Baslé
 * @since 2.3
 */
public class CredentialsManager {

    private final Map<String, String> bucketCredentials = new HashMap<String, String>();

    private Tuple2<String, String> clusterCredentials = Tuple.create(null, null);

    public CredentialsManager addBucketCredential(String bucket, String password) {
        bucketCredentials.put(bucket, password);
        return this;
    }

    public CredentialsManager addClusterCredentials(String login, String password) {
        clusterCredentials = Tuple.create(login, password);
        return this;
    }

    public String[][] getCredentials(AuthenticationContext context, String specific) {
        if (context == BUCKET_KEYVALUE || context == BUCKET_N1QL) {
            String[] cred = new String[2];
            cred[0] = specific;
            cred[1] = bucketCredentials.get(specific);
            return new String[][]{cred};
        } else if (context == CLUSTER_N1QL || context == CLUSTER_FTS) {
            String[][] creds = new String[bucketCredentials.size()][];
            int i = 0;
            for (Map.Entry<String, String> entry : bucketCredentials.entrySet()) {
                String[] cred = new String[2];
                cred[0] = entry.getKey();
                cred[1] = entry.getValue();
                creds[i++] = cred;
            }
            return creds;
        } else if (context == CLUSTER_MANAGEMENT) {
            String[] cred = new String[2];
            cred[0] = clusterCredentials.value1();
            cred[1] = clusterCredentials.value2();
            return new String[][] { cred };
        }
        throw new UnsupportedOperationException("Authentication context " + context + " is currently unsupported");
    }

    public String getDefaultBucketPassword() {
            return "";
    }

    public String resolveBucketPassword(final String bucketName, final String optionalExplicitPassword) {
        if (optionalExplicitPassword != null) {
            return optionalExplicitPassword;
        }

        return getBucketPasswordOrDefault(bucketName);
    }

    protected String getBucketPasswordOrDefault(final String bucketName) {
        // try to get it from registered credentials
        String registeredPassword = bucketCredentials.get(bucketName);
        if (registeredPassword != null) {
            return registeredPassword;
        }

        return getDefaultBucketPassword();
    }
}
