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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.vbucket.ConfigurationProvider;
import com.couchbase.client.vbucket.ConfigurationProviderMemcacheMock;
import com.couchbase.client.vbucket.CouchbaseNodeOrder;
import com.couchbase.client.vbucket.config.Config;
import net.spy.memcached.TestConfig;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfBucketIsNull() throws Exception {
    new CouchbaseConnectionFactory(uris, null, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfBucketIsEmpty() throws Exception {
    new CouchbaseConnectionFactory(uris, "", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfPasswordIsNull() throws Exception {
    new CouchbaseConnectionFactory(uris, "default", null);
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
   * {@link com.couchbase.client.CouchbaseConnectionFactory#pastReconnThreshold()}
   * is called in longer frames than the time period allows, no configuration update
   * is triggered.
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

  @Test
  @Ignore
  public void shouldRandomizeNodeList() throws Exception {
    ConfigurationProviderMemcacheMock providerMock = new ConfigurationProviderMemcacheMock(
      Arrays.asList("127.0.0.1:8091/pools", "127.0.0.2:8091/pools",
        "127.0.0.3:8091/pools", "127.0.0.4:8091/pools"), "default"
    );

    CouchbaseConnectionFactory connFact = new CouchbaseConnectionFactoryMock(
      Arrays.asList(
        new URI("http://127.0.0.1:8091/pools"), new URI("http://127.0.0.2:8091/pools"),
        new URI("http://127.0.0.3:8091/pools"), new URI("http://127.0.0.5:8091/pools")
      ), "default", "", providerMock, CouchbaseNodeOrder.RANDOM
    );

    List<URI> oldList = connFact.getStoredBaseList();
    int oIndex1 = oldList.indexOf(new URI("http://127.0.0.1:8091/pools"));
    int oIndex2 = oldList.indexOf(new URI("http://127.0.0.2:8091/pools"));
    int oIndex3 = oldList.indexOf(new URI("http://127.0.0.3:8091/pools"));
    int oIndex4 = oldList.indexOf(new URI("http://127.0.0.5:8091/pools"));

    int tries = 100;
    for(int i = 0; i < tries; i++) {
      //connFact.updateStoredBaseList(connFact.getVBucketConfig());
      assertTrue(providerMock.baseListUpdated);
      List<URI> newList = connFact.getStoredBaseList();
      System.out.println("old: " + oldList);
      System.out.println("new: " + newList);
      int nIndex1 = newList.indexOf(new URI("http://127.0.0.1:8091/pools"));
      int nIndex2 = newList.indexOf(new URI("http://127.0.0.2:8091/pools"));
      int nIndex3 = newList.indexOf(new URI("http://127.0.0.3:8091/pools"));
      int nIndex4 = newList.indexOf(new URI("http://127.0.0.5:8091/pools"));
      if (oIndex1 != nIndex1 || oIndex2 != nIndex2 || oIndex3 != nIndex3 || oIndex4 == nIndex4) {
        assertTrue(true);
        return;
      }
    }

    assertTrue("Node list was not different after " + tries + " tries", false);
  }

  @Test
  public void shouldBootstrapThroughProperties() throws Exception {
    System.setProperty("cbclient.nodes", "http://" + TestConfig.IPV4_ADDR
      + ":8091/pools");
    System.setProperty("cbclient.bucket", "default");
    System.setProperty("cbclient.password", "");

    CouchbaseConnectionFactory factory = new CouchbaseConnectionFactory();
    Config config = factory.getVBucketConfig();

    assertTrue(config.getServersCount() > 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfNoNodeProperty() throws Exception {
    System.clearProperty("cbclient.nodes");
    System.setProperty("cbclient.bucket", "default");
    System.setProperty("cbclient.password", "");

    new CouchbaseConnectionFactory();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfNoBucketProperty() throws Exception {
    System.clearProperty("cbclient.bucket");
    System.setProperty("cbclient.password", "");
    System.setProperty("cbclient.nodes", "http://" + TestConfig.IPV4_ADDR
      + ":8091/pools");

    new CouchbaseConnectionFactory();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfNoPasswordProperty() throws Exception {
    System.clearProperty("cbclient.password");
    System.setProperty("cbclient.bucket", "default");
    System.setProperty("cbclient.nodes", "http://" + TestConfig.IPV4_ADDR
      + ":8091/pools");

    new CouchbaseConnectionFactory();
  }

}
