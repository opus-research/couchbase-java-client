package com.couchbase.client.java.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.InvalidPasswordException;
import com.couchbase.client.java.util.TestProperties;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests around the usage of a {@link PasswordAuthenticator}.
 */
public class PasswordAuthenticationTest {

    private Cluster cluster;
    private PasswordAuthenticator authenticator;

    @Before
    public void init() {
        this.authenticator = new PasswordAuthenticator();
        this.cluster = CouchbaseCluster.create(TestProperties.seedNode())
                .authenticate(authenticator);
    }

    @After
    public void tearDown() {
        this.cluster.disconnect();
    }

    @Test
    public void testClusterManagementWithGoodCreds() {
        authenticator.cluster(TestProperties.adminName(), TestProperties.adminPassword());

        ClusterManager manager = cluster.clusterManager();

        assertThat(manager).isNotNull();
        assertThat(manager.info()).isNotNull();
    }

    @Test
    public void testClusterManagementWithBadCreds() {
        authenticator.cluster(TestProperties.adminName(), TestProperties.adminPassword() + "bad");

        ClusterManager manager = cluster.clusterManager();
        try {
            manager.info();
            fail("Expected InvalidPasswordException");
        } catch (InvalidPasswordException e) {
            //success
        }
    }

    @Test
    public void testOpenBucketWithBucketNameAndWrongCreds() {
        authenticator.bucket(TestProperties.bucket(), TestProperties.password() + "bad");

        try {
            cluster.openBucket(TestProperties.bucket());
            fail("Expected InvalidPasswordException");
        } catch (InvalidPasswordException e) {
            //success
        }

        try {
            cluster.openBucket(TestProperties.bucket(), 5, TimeUnit.SECONDS);
            fail("Expected InvalidPasswordException");
        } catch (InvalidPasswordException e) {
            //success
        }
    }

    @Test
    public void testOpenBucketWithBucketNameAndGoodCreds() {
        authenticator.bucket(TestProperties.bucket(), TestProperties.password());

        assertThat(cluster.openBucket(TestProperties.bucket())).isNotNull();
        assertThat(cluster.openBucket(TestProperties.bucket(), 5, TimeUnit.SECONDS)).isNotNull();
    }
    @Test
    public void testOpenBucketDefaultAndWrongCreds() {
        authenticator.bucket("default", "bad");

        try {
            cluster.openBucket();
            fail("Expected InvalidPasswordException");
        } catch (InvalidPasswordException e) {
            //success
        }

        try {
            cluster.openBucket(5, TimeUnit.SECONDS);
            fail("Expected InvalidPasswordException");
        } catch (InvalidPasswordException e) {
            //success
        }
    }

    @Test
    public void testOpenBucketDefaultWithGoodCreds() {
        authenticator.bucket("default", "");

        assertThat(cluster.openBucket()).isNotNull();
        assertThat(cluster.openBucket(5, TimeUnit.SECONDS)).isNotNull();
    }

    @Test
    public void testOpenBucketWithExplicitCredsDoesntOverwriteAuthenticator() {
        authenticator.bucket(TestProperties.bucket(), TestProperties.password());

        try {
            cluster.openBucket(TestProperties.bucket(), "bad");
            fail("Expected InvalidPasswordException, usage of explicit password");
        } catch (InvalidPasswordException e) {
            //success
        }
        assertThat(authenticator.getCredentials(CredentialContext.BUCKET_KV, TestProperties.bucket()))
                .containsOnly(new Credential(TestProperties.bucket(), TestProperties.password()));
    }

    @Test
    public void testClusterManagerWithExplicitCredsDoesntOverwriteAuthenticator() {
        authenticator.cluster(TestProperties.adminName(), TestProperties.adminPassword());

        ClusterManager manager = cluster.clusterManager(TestProperties.adminName(), "bad");
        try {
            manager.info();
            fail("Expected InvalidPasswordException, usage of explicit password");
        } catch (InvalidPasswordException e) {
            //success
        }
        assertThat(authenticator.getCredentials(CredentialContext.CLUSTER_MANAGEMENT, TestProperties.adminName()))
                .containsOnly(new Credential(TestProperties.adminName(), TestProperties.adminPassword()));
    }

    @Test
    public void shouldAllowToResetAuthenticator() {
        Authenticator auth1 = cluster.authenticator();
        Authenticator auth2 = new PasswordAuthenticator();

        Assertions.assertThat(cluster.authenticator()).isSameAs(auth1);

        cluster.authenticate(auth2);

        Assertions.assertThat(cluster.authenticator())
                .isNotSameAs(auth1)
                .isSameAs(auth2);
    }

    @Test
    public void shouldKeepOldAuthenticatorOnOpenedBucket() {
        Authenticator auth1 = new PasswordAuthenticator().bucket(TestProperties.bucket(), TestProperties.password())
                .cluster("foo", "foo");
        Authenticator auth2 = new PasswordAuthenticator().bucket(TestProperties.bucket(), TestProperties.password())
                .cluster("bar", "bar");

        cluster.authenticate(auth1);
        Bucket b = cluster.openBucket(TestProperties.bucket());
        cluster.authenticate(auth2);

        assertThat(b.authenticator()).isSameAs(auth1);
        assertThat(cluster.authenticator()).isNotSameAs(auth1);

        Bucket bCached = cluster.openBucket(TestProperties.bucket());

        assertThat(bCached.authenticator()).isSameAs(auth1);
        assertThat(bCached.authenticator().getCredentials(CredentialContext.CLUSTER_MANAGEMENT, null))
                .containsOnly(new Credential("foo", "foo"));


        bCached.close();
        Bucket bReopened = cluster.openBucket(TestProperties.bucket());

        assertThat(bCached.isClosed()).isTrue();
        assertThat(b.isClosed()).isTrue();
        assertThat(bReopened)
                .isNotSameAs(b)
                .isNotSameAs(bCached);
        assertThat(bReopened.authenticator())
                .isSameAs(cluster.authenticator())
                .isSameAs(auth2);
        assertThat(bReopened.authenticator().getCredentials(CredentialContext.CLUSTER_MANAGEMENT, null))
                .containsOnly(new Credential("bar", "bar"));
    }
}