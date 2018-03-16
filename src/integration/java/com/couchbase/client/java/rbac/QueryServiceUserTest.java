package com.couchbase.client.java.rbac;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.PasswordAuthenticator;
import com.couchbase.client.java.cluster.UserRole;
import com.couchbase.client.java.cluster.UserSettings;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.util.CouchbaseTestContext;
import com.couchbase.client.java.util.features.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Subhashni Balakrishnan
 */
public class QueryServiceUserTest {
    private static CouchbaseTestContext ctx;
    private static String username = "queryServiceUser";
    private static String password = "password";
    private static Bucket bucket;
    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws Exception {
        ctx = CouchbaseTestContext.builder()
                .build()
                .ignoreIfClusterUnder(Version.parseVersion("5.0.0"))
                .ignoreIfNoN1ql()
                .ensurePrimaryIndex();

        ctx.clusterManager().upsertUser(username, UserSettings.build().password(password)
                .roles(Arrays.asList(new UserRole("query_select", ctx.bucketName()),
                        new UserRole("data_monitoring", ctx.bucketName()))));//open bug MB-23475, needs a data role else cccp will fail
        cluster = CouchbaseCluster.create(ctx.seedNode());
        cluster.authenticate(new PasswordAuthenticator(username, password));
        Thread.sleep(100); //sleep a bit for the user to be async updated to memcached before opening bucket
        bucket = cluster.openBucket(ctx.bucketName());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cluster.disconnect();
        ctx.clusterManager().removeUser(username);
        ctx.destroyBucketAndDisconnect();
    }

    @Test
    public void testN1qlSelectAuth() {
        N1qlQueryResult result = bucket.query(N1qlQuery.simple("select * from " + ctx.bucketName() + " limit 1"));
        assertEquals("N1ql select should be successful", true, result.finalSuccess());
    }

    @Test
    @Ignore //fails currently
    public void testN1qlInsertAuthFail() {
        N1qlQueryResult result = bucket.query(N1qlQuery.simple("INSERT INTO "+ ctx.bucketName() +" (KEY, VALUE) VALUES (\"foo\", \n" +
                "      { \"bar\": \"baz\" }) RETURNING * \n "));
        assertEquals("N1ql insert should not be successful", 1, result.errors().size());
    }
}