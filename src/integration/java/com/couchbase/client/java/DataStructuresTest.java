package com.couchbase.client.java;

import static com.googlecode.catchexception.CatchException.caughtException;
import static com.googlecode.catchexception.CatchException.verifyException;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.util.CouchbaseTestContext;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
public class DataStructuresTest {
	private static CouchbaseTestContext ctx;
	private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DataStructuresTest.class);


	@BeforeClass
	public static void connect() throws Exception {
		ctx = CouchbaseTestContext.builder()
				.bucketQuota(100)
				.bucketReplicas(1)
				.bucketType(BucketType.COUCHBASE)
				.build();

		ctx.ignoreIfMissing(CouchbaseFeature.SUBDOC);
	}

	@AfterClass
	public static void disconnect() throws InterruptedException {
		ctx.destroyBucketAndDisconnect();
		ctx.disconnect();
	}

	@Test
	public void testList() {
		ctx.bucket().async().listPush("list", 1).toBlocking().single();
		ctx.bucket().async().listPush("list", "abc").toBlocking().single();
		int firstVal = ctx.bucket().async().listGet("list", 0, Integer.class).toBlocking().single();
		assertEquals(firstVal, 1);
		String secondVal = ctx.bucket().async().listGet("list", 1, String.class).toBlocking().single();
		assertEquals(secondVal, "abc");

	}

}
