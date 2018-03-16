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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.error.BucketDoesNotExistException;
import com.couchbase.client.java.error.InvalidPasswordException;
import com.couchbase.client.java.util.TestProperties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Basic test cases which verify functionality not bound to a {@link Bucket}.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public class ConnectionTest  {

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfBucketIsNull() {
        Cluster cluster = CouchbaseCluster.create(TestProperties.seedNode());
        cluster.openBucket(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfBucketIsEmpty() {
        Cluster cluster = CouchbaseCluster.create(TestProperties.seedNode());
        cluster.openBucket("");
    }

    @Test(expected = BucketDoesNotExistException.class)
    public void shouldThrowConfigurationExceptionForWrongBucketName() {
        Cluster cluster = CouchbaseCluster.create(TestProperties.seedNode());
        cluster.openBucket("someWrongBucketName");
    }

    @Test(expected = InvalidPasswordException.class)
    public void shouldThrowConfigurationExceptionForWrongBucketPassword() {
        Cluster cluster = CouchbaseCluster.create(TestProperties.seedNode());
        cluster.openBucket(TestProperties.bucket(), "completelyWrongPassword");
    }

    @Test
    public void shouldConnectAfterFailedAttempt() {
        Cluster cluster = CouchbaseCluster.create(TestProperties.seedNode());
        try {
            cluster.openBucket("protected");
        } catch (CouchbaseException e) {
            System.out.println(e);
        }
        cluster.credentialsManager().addBucketCredential("protected", "protected");
        Bucket b = cluster.openBucket("protected");
        assertNotNull(b);
    }

    @Test
    public void shouldCacheBucketReference() {
        Cluster cluster = CouchbaseCluster.create(TestProperties.seedNode());
        Bucket bucket1 = cluster.openBucket(TestProperties.bucket(), TestProperties.password());
        Bucket bucket2 = cluster.openBucket(TestProperties.bucket(), TestProperties.password());

        assertEquals(bucket1.hashCode(), bucket2.hashCode());

        assertFalse(bucket1.isClosed());
        assertFalse(bucket2.isClosed());
        bucket1.close();
        assertTrue(bucket1.isClosed());
        assertTrue(bucket2.isClosed());

        Bucket bucket3 = cluster.openBucket(TestProperties.bucket(), TestProperties.password());

        assertNotEquals(bucket1.hashCode(), bucket3.hashCode());

        assertFalse(bucket3.isClosed());
    }
}
