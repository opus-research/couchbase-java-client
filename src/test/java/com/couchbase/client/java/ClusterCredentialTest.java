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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;

/**
 * Unit tests around credential management in the {@link Cluster}, through {@link CredentialsManager}.
 *
 * @author Simon Baslé
 * @since 2.3
 */
public class ClusterCredentialTest {

    private CredentialsManager spyCredentialsManager;
    private CouchbaseAsyncCluster mockCluster;
    private CouchbaseCluster syncCluster;

    @Before
    public void prepareTest() {
        spyCredentialsManager = spy(new CredentialsManager());
        mockCluster = mock(CouchbaseAsyncCluster.class);
        syncCluster = spy(
                new CouchbaseCluster(mock(CouchbaseEnvironment.class), mock(ConnectionString.class), false) {
                    @Override
                    protected CouchbaseAsyncCluster createAsyncCluster(boolean sharedEnvironment) {
                        return mockCluster;
                    }
                });
        AsyncBucket mockAsyncBucket = mock(CouchbaseAsyncBucket.class);
        ClusterFacade mockCore = mock(ClusterFacade.class);

        when(spyCredentialsManager.getDefaultBucketPassword()).thenReturn("ZZZ");
        when(mockCluster.credentialsManager()).thenReturn(spyCredentialsManager);
        when(mockCluster.getCachedBucket(anyString())).thenReturn(null);
        when(mockCluster.openAndInstantiateBucket(anyString(), anyString(), anyList()))
                .thenReturn(Observable.just(mockAsyncBucket));
        when(mockCluster.core()).thenReturn(Observable.just(mockCore));

        when(mockCluster.clusterManager()).thenCallRealMethod();
        when(mockCluster.clusterManager(anyString(), anyString())).thenCallRealMethod();
        when(mockCluster.openBucket(anyString())).thenCallRealMethod();
        when(mockCluster.openBucket(anyString(), anyString())).thenCallRealMethod();
        when(mockCluster.openBucket(anyString(), anyString(), anyList())).thenCallRealMethod();
    }

    @Test
    public void asyncClusterManagerNoCredentials() {
        // ensure the fact that opening a Bucket and ClusterManager does not initialize credentials
        syncCluster.clusterManager("A", "PA");
        syncCluster.openBucket("B", "PB");

        String[][] creds = spyCredentialsManager.getCredentials(AuthenticationContext.CLUSTER_MANAGEMENT, null);
        assertNotNull(creds);
        assertEquals(1, creds.length);
        assertNotNull(creds[0]);
        assertEquals(2, creds[0].length);
        assertNull(creds[0][0]);
        assertNull(creds[0][1]);

        creds = spyCredentialsManager.getCredentials(AuthenticationContext.BUCKET_KEYVALUE, "B");
        assertNotNull(creds);
        assertEquals(1, creds.length);
        assertNotNull(creds[0]);
        assertEquals(2, creds[0].length);
        assertEquals("B", creds[0][0]);
        assertNull(creds[0][1]);

        creds = spyCredentialsManager.getCredentials(AuthenticationContext.CLUSTER_N1QL, null);
        assertNotNull(creds);
        assertEquals(0, creds.length);

        verify(spyCredentialsManager, never()).addBucketCredential(anyString(), anyString());
        verify(spyCredentialsManager, never()).addClusterCredentials(anyString(), anyString());
    }

    @Test
    public void asyncClusterManagerUsesCredentials() {
        final AtomicReference<String[][]> answer = new AtomicReference<String[][]>();
        when(spyCredentialsManager.getCredentials(AuthenticationContext.CLUSTER_MANAGEMENT, null))
                .thenAnswer(new Answer<String[][]>() {
                    @Override
                    public String[][] answer(InvocationOnMock invocation) throws Throwable {
                        answer.set((String[][]) invocation.callRealMethod());
                        return answer.get();
                    }
                });

        mockCluster.credentialsManager().addClusterCredentials("clusterA", "secureCluster");
        mockCluster.clusterManager(); //trigger the preparation of observable

        verify(spyCredentialsManager).getCredentials(AuthenticationContext.CLUSTER_MANAGEMENT, null);
        verify(spyCredentialsManager, atMost(1)).getCredentials(any(AuthenticationContext.class), anyString());

        String[][] creds = answer.get();
        assertNotNull(creds);
        assertEquals(1, creds.length);
        assertNotNull(creds[0]);
        assertEquals(2, creds[0].length);
        assertEquals("clusterA", creds[0][0]);
        assertEquals("secureCluster", creds[0][1]);
    }

    @Test
    public void asyncBucketNoCredentialsUsesDefaultPassword() {
        mockCluster.openBucket("A");

        verify(mockCluster).openBucket("A");
        verify(mockCluster).openBucket("A", null, null);

        verify(spyCredentialsManager, never()).getCredentials(any(AuthenticationContext.class), anyString());
        verify(spyCredentialsManager).resolveBucketPassword("A", null);
        verify(spyCredentialsManager).getBucketPasswordOrDefault("A"); //didn't use null
        verify(spyCredentialsManager).getDefaultBucketPassword(); //used default password

        verify(mockCluster).openAndInstantiateBucket(eq("A"), eq("ZZZ"), anyList());
    }

    @Test
    public void asyncBucketNoCredentialsExplicitPasswordUsesIt() {
        mockCluster.openBucket("B", "secure");

        verify(mockCluster).openBucket("B", "secure");
        verify(mockCluster).openBucket("B", "secure", null);

        verify(spyCredentialsManager, never()).getCredentials(any(AuthenticationContext.class), anyString());
        verify(spyCredentialsManager).resolveBucketPassword("B", "secure");
        verify(spyCredentialsManager, never()).getBucketPasswordOrDefault(anyString());
        verify(spyCredentialsManager, never()).getDefaultBucketPassword();

        verify(mockCluster).openAndInstantiateBucket(eq("B"), eq("secure"), anyList());
    }

    @Test
    public void asyncBucketWithCredentialsButExplicitPasswordUsesIt() {
        mockCluster.credentialsManager().addBucketCredential("B", "unsecure");
        mockCluster.openBucket("B", "secure");

        verify(mockCluster).openBucket("B", "secure");
        verify(mockCluster).openBucket("B", "secure", null);

        verify(spyCredentialsManager, never()).getCredentials(any(AuthenticationContext.class), anyString());
        verify(spyCredentialsManager).resolveBucketPassword("B", "secure");
        verify(spyCredentialsManager, never()).getBucketPasswordOrDefault(anyString());
        verify(spyCredentialsManager, never()).getDefaultBucketPassword();

        verify(mockCluster).openAndInstantiateBucket(eq("B"), eq("secure"), anyList());
    }

    @Test
    public void asyncBucketWithCredentialsUsesIt() {
        mockCluster.credentialsManager().addBucketCredential("C", "protected");
        mockCluster.openBucket("C");

        verify(mockCluster).openBucket("C");
        verify(mockCluster).openBucket("C", null, null);

        verify(spyCredentialsManager, never()).getCredentials(any(AuthenticationContext.class), anyString());
        verify(spyCredentialsManager).resolveBucketPassword("C", null);
        verify(spyCredentialsManager).getBucketPasswordOrDefault("C");
        verify(spyCredentialsManager, never()).getDefaultBucketPassword();

        verify(mockCluster).openAndInstantiateBucket(eq("C"), eq("protected"), anyList());
    }

    @Test
    public void syncClusterManagerUsesCredentials() {
        final AtomicReference<String[][]> answer = new AtomicReference<String[][]>();
        when(spyCredentialsManager.getCredentials(AuthenticationContext.CLUSTER_MANAGEMENT, null))
                .thenAnswer(new Answer<String[][]>() {
                    @Override
                    public String[][] answer(InvocationOnMock invocation) throws Throwable {
                        answer.set((String[][]) invocation.callRealMethod());
                        return answer.get();
                    }
                });

        syncCluster.credentialsManager().addClusterCredentials("clusterA", "secureCluster");
        syncCluster.clusterManager(); //trigger the preparation of observable

        verify(spyCredentialsManager).getCredentials(AuthenticationContext.CLUSTER_MANAGEMENT, null);
        verify(spyCredentialsManager, atMost(1)).getCredentials(any(AuthenticationContext.class), anyString());

        String[][] creds = answer.get();
        assertNotNull(creds);
        assertEquals(1, creds.length);
        assertNotNull(creds[0]);
        assertEquals(2, creds[0].length);
        assertEquals("clusterA", creds[0][0]);
        assertEquals("secureCluster", creds[0][1]);
    }

    @Test
    public void syncBucketNoCredentialsUsesDefaultPassword() {
        syncCluster.openBucket("A");

        verify(syncCluster).openBucket("A");
        verify(syncCluster).openBucket("A", null, null);

        verify(mockCluster).openBucket("A", "ZZZ", null);

        verify(spyCredentialsManager, never()).getCredentials(any(AuthenticationContext.class), anyString());
        //sync resolves the password first, to default, then async uses it as if it was explicit
        verify(spyCredentialsManager).resolveBucketPassword("A", null);
        verify(spyCredentialsManager).resolveBucketPassword("A", "ZZZ");
        verify(spyCredentialsManager).getBucketPasswordOrDefault("A");
        verify(spyCredentialsManager).getDefaultBucketPassword();

        verify(mockCluster).openAndInstantiateBucket(eq("A"), eq("ZZZ"), anyList());

        //additionally, check no more interactions for non-generic checks
        verify(spyCredentialsManager, times(1)).resolveBucketPassword(anyString(), anyString());
        verify(spyCredentialsManager, times(1)).getBucketPasswordOrDefault(anyString());
        verify(mockCluster, times(1)).openAndInstantiateBucket(anyString(), anyString(), anyList());
    }

    @Test
    public void syncBucketNoCredentialsExplicitPasswordUsesIt() {
        syncCluster.openBucket("B", "secure");

        verify(syncCluster).openBucket("B", "secure");
        verify(syncCluster).openBucket("B", "secure", null);

        verify(mockCluster).openBucket("B", "secure", null);

        verify(spyCredentialsManager, never()).getCredentials(any(AuthenticationContext.class), anyString());
        //both sync and async used the explicit password, but no more than twice
        verify(spyCredentialsManager, times(2)).resolveBucketPassword("B", "secure");
        verify(spyCredentialsManager, never()).getBucketPasswordOrDefault(anyString());
        verify(spyCredentialsManager, never()).getDefaultBucketPassword();

        verify(mockCluster).openAndInstantiateBucket(eq("B"), eq("secure"), anyList());

        //additionally, check no more interactions for non-generic checks
        verify(spyCredentialsManager, times(2)).resolveBucketPassword(anyString(), anyString());
        verify(mockCluster, times(1)).openAndInstantiateBucket(anyString(), anyString(), anyList());
    }

    @Test
    public void syncBucketWithCredentialsButExplicitPasswordUsesIt() {
        syncCluster.credentialsManager().addBucketCredential("B", "unsecure");
        syncCluster.openBucket("B", "secure");

        verify(syncCluster).openBucket("B", "secure");
        verify(syncCluster).openBucket("B", "secure", null);

        verify(mockCluster).openBucket("B", "secure", null);

        verify(spyCredentialsManager, never()).getCredentials(any(AuthenticationContext.class), anyString());

        //both sync and async used the explicit password
        verify(spyCredentialsManager, times(2)).resolveBucketPassword("B", "secure");
        verify(spyCredentialsManager, never()).getBucketPasswordOrDefault(anyString());
        verify(spyCredentialsManager, never()).getDefaultBucketPassword();

        verify(mockCluster).openAndInstantiateBucket(eq("B"), eq("secure"), anyList());

        //additionally, check no more interactions for non-generic checks
        verify(spyCredentialsManager, times(2)).resolveBucketPassword(anyString(), anyString());
        verify(mockCluster, times(1)).openAndInstantiateBucket(anyString(), anyString(), anyList());
    }

    @Test
    public void syncBucketWithCredentialsUsesIt() {
        syncCluster.credentialsManager().addBucketCredential("C", "protected");
        syncCluster.openBucket("C");

        verify(syncCluster).openBucket("C");
        verify(syncCluster).openBucket("C", null, null);

        verify(mockCluster).openBucket("C", "protected", null);

        verify(spyCredentialsManager, never()).getCredentials(any(AuthenticationContext.class), anyString());
        //sync + async both resolved, but different passwords
        //only sync checked the password in credentialsManager
        verify(spyCredentialsManager).resolveBucketPassword("C", null);
        verify(spyCredentialsManager).resolveBucketPassword("C", "protected");
        verify(spyCredentialsManager).getBucketPasswordOrDefault("C");

        verify(spyCredentialsManager, never()).getDefaultBucketPassword();
        verify(mockCluster).openAndInstantiateBucket(eq("C"), eq("protected"), anyList());

        //additionally, check no more interactions for non-generic checks
        verify(spyCredentialsManager, times(2)).resolveBucketPassword(anyString(), anyString());
        verify(spyCredentialsManager, times(1)).getBucketPasswordOrDefault(anyString());
        verify(mockCluster, times(1)).openAndInstantiateBucket(anyString(), anyString(), anyList());
    }
}