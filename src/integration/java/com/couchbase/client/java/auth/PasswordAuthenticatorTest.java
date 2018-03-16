package com.couchbase.client.java.auth;

import java.util.ArrayList;
import java.util.Arrays;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.UserRole;
import com.couchbase.client.java.cluster.UserSettings;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.error.InvalidPasswordException;
import com.couchbase.client.java.error.MixedAuthenticationException;
import com.couchbase.client.java.transcoder.Transcoder;
import com.couchbase.client.java.util.CouchbaseTestContext;
import com.couchbase.client.java.util.features.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Subhashni Balakrishnan
 */
public class PasswordAuthenticatorTest {
    private static CouchbaseTestContext ctx;
    private static String username = "testUser";
    private static String password = "password";

    @BeforeClass
    public static void setup() throws Exception {
        ctx = CouchbaseTestContext.builder()
                .build()
                .ignoreIfClusterUnder(Version.parseVersion("5.0.0"));

        ctx.clusterManager().upsertUser(username, UserSettings.build().password(password)
                .roles(Arrays.asList(new UserRole("data_reader_writer", "*"))));
        Thread.sleep(5000); //sleep a bit for the user to be async updated to memcached before opening bucket
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ctx.clusterManager().removeUser(username);
        ctx.destroyBucketAndDisconnect();
    }

    @Test
    public void shouldOpenBucketWithCorrectCredentials() {
        Cluster cluster = CouchbaseCluster.create(ctx.seedNode());
        cluster.authenticate(new PasswordAuthenticator(username, password));
        cluster.openBucket(ctx.bucketName());
        cluster.disconnect();
    }

    @Test(expected = InvalidPasswordException.class)
    public void shouldNotOpenBucketWithInCorrectCredentials() {
        Cluster cluster = CouchbaseCluster.create(ctx.seedNode());
        cluster.authenticate(new PasswordAuthenticator(username, "x"));
        cluster.openBucket(ctx.bucketName());
    }

    @Test(expected = InvalidPasswordException.class)
    public void shouldNotOpenBucketWithInCorrectCredentialsOverload() {
        Cluster cluster = CouchbaseCluster.create(ctx.seedNode());
        cluster.authenticate(new PasswordAuthenticator(username, "x"));
        cluster.openBucket(ctx.bucketName(), new ArrayList<Transcoder<? extends Document, ?>>());
    }

    @Test(expected = MixedAuthenticationException.class)
    public void shouldNotAcceptMixedAuthenticationWithBucketCredentials() {
        Cluster cluster = CouchbaseCluster.create(ctx.seedNode());
        cluster.authenticate(new PasswordAuthenticator(username, password));
        cluster.openBucket(ctx.bucketName(), "x");
    }

    @Test(expected = MixedAuthenticationException.class)
    public void shouldNotAcceptMixedAuthenticationWithBucketCredentialsOverload() {
        Cluster cluster = CouchbaseCluster.create(ctx.seedNode());
        cluster.authenticate(new PasswordAuthenticator(username, password));
        cluster.openBucket(ctx.bucketName(), "x", null);
    }

    @Test(expected = MixedAuthenticationException.class)
    public void shouldNotAcceptMixedAuthenticationWithClassicAuthenticatorOverload() {
        Cluster cluster = CouchbaseCluster.create(ctx.seedNode());
        cluster.authenticate(new PasswordAuthenticator(username, password));
        cluster.authenticate(new ClassicAuthenticator());
    }

    @Test
    public void shouldWorkWithUserNameInConnectionString() {
        Cluster cluster = CouchbaseCluster.fromConnectionString("couchbase://" + username + "@" + ctx.seedNode());
        cluster.authenticate(new PasswordAuthenticator(password));
        cluster.openBucket(ctx.bucketName());
        cluster.disconnect();
    }

	@Test
	public void shouldWorkWithUserNameInConnectionStringWithCorrectPriority() {
		Cluster cluster = CouchbaseCluster.fromConnectionString("couchbase://" + "blah" + "@" + ctx.seedNode());
		cluster.authenticate(new PasswordAuthenticator(username, password));
		cluster.openBucket(ctx.bucketName());
		cluster.disconnect();
	}
}