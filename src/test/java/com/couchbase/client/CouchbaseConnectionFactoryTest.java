/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2013 Couchbase, Inc.
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
package com.couchbase.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.TestConfig;

import org.junit.Before;
import org.junit.Test;

import com.couchbase.client.vbucket.ConfigurationProvider;
import com.couchbase.client.vbucket.ConfigurationProviderMock;

/**
 * Makes sure the CouchbaseConnectionFactory works as expected.
 */
public class CouchbaseConnectionFactoryTest {

  private List<URI> uris;
  private CouchbaseConnectionFactoryBuilder instance;

  @Before
  public void setUp() {
    instance = new CouchbaseConnectionFactoryBuilder();
    uris = Arrays.asList(URI.create("http://" + TestConfig.IPV4_ADDR
      + ":8091/pools"));
  }

  private CouchbaseConnectionFactory buildFactory() throws IOException {
    return instance.buildCouchbaseConnection(uris, "default", "");
  }

  /**
   * Make sure that the first calls to pastReconnThreshold() yield false
   * and the first one who is over getMaxConfigCheck() yields true.
   *
   * @throws IOException
   */
  @Test
  public void testPastReconnectThreshold() throws IOException {
    CouchbaseConnectionFactory connFact = buildFactory();

    for(int i=1; i<connFact.getMaxConfigCheck(); i++) {
      boolean pastReconnThreshold = connFact.pastReconnThreshold();
      assertFalse(pastReconnThreshold);
    }

    boolean pastReconnThreshold = connFact.pastReconnThreshold();
    assertTrue(pastReconnThreshold);
  }

  /**
   * Verifies that when
   * {@link CouchbaseConnectionFactory#pastReconnectThreshold()} is called
   * in longer frames than the time period allows, no configuration update is
   * triggered.
   */
  @Test
  public void testPastReconnectThresholdWithSleep() throws Exception {
    CouchbaseConnectionFactory connFact = buildFactory();

    for(int i=1; i<=connFact.getMaxConfigCheck()-1; i++) {
      boolean pastReconnThreshold = connFact.pastReconnThreshold();
      assertFalse(pastReconnThreshold);
    }

    Thread.sleep(TimeUnit.SECONDS.toMillis(11));

    for(int i=1; i<=connFact.getMaxConfigCheck()-1; i++) {
      boolean pastReconnThreshold = connFact.pastReconnThreshold();
      assertFalse(pastReconnThreshold);
    }
  }

  /**
   * Tests the correctness of the initialization and shutdown phase
   * of the Memcached connection.
   *
   * @pre Create a list of array of addresses and get a connection
   * factory instance from them. Create memcached connection using the
   * parameters as above.
   * @post Assert true if the memcached connection is alive.
   * Shutdown the client and then again check its alive.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testMemcachedConnection() throws IOException, InterruptedException {
    CouchbaseConnectionFactory cf = buildFactory();

    List<InetSocketAddress> addrs = AddrUtil.getAddressesFromURL(
      cf.getVBucketConfig().getCouchServers()
    );

    MemcachedConnection memConn = cf.createConnection(addrs);
    assertTrue(memConn.isAlive());
    memConn.shutdown();
    Thread.sleep(5000);
    assertFalse(memConn.isAlive());
  }

  /**
   * Tests the VbucketConfig retrieved from connection factory.
   *
   * @pre Create a list of array of addresses and get a connection
   * factory instance from them. Retrieve VBucket config from the same.
   * @post Assert the message if it failed to retrieve.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testVBucketConfig() throws IOException , InterruptedException {
    CouchbaseConnectionFactory cf = buildFactory();
    assert  cf.getVBucketConfig() == null
      : "Couldn't retrieve VBucket Config";
    assert  cf.getVBucketConfig().getServersCount() == 0
      : "Couldn't retrieve server nodes in the configuration";
  }

  /**
   * Resubscriber Backoff.
   *
   * @pre Prepare the base list which is an invalid URL.
   * Create a mock configuration provider instance and pass it to
   * couchbase connection factory. Create instance of Resubscriber
   * and run it.
   * @post Test succeeds to ensure that resubscriber runs once
   * and then backs off.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Test
  public void testResubscriberBackOff() throws IOException {

      List<URI> baseList = new ArrayList<URI>();
      baseList.add(URI.create("http://"+"badurl"+":8091/pools"));
      ConfigurationProvider provider = new ConfigurationProviderMock();

      CouchbaseConnectionFactoryMock factory;
      factory = new CouchbaseConnectionFactoryMock(
        baseList,
        "resubscriber-bucket",
        "",
        provider
      );
      CouchbaseConnectionFactoryMock.Resubscriber resubscriber = factory.new Resubscriber(provider);
      resubscriber.run();
      assertNotNull(factory.getWaitTime());
  }
}