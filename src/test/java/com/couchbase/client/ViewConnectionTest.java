/**
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
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.TestConfig;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Verifies the correct functionality of the ViewConnection class.
 */
public class ViewConnectionTest {

  /**
   * Tests the correctness of the initialization and shutdown phase.
   *
   * @pre Create a list of array of addresses and get a connection
   * factory instance from them. Create view connection using the
   * parameters as above.
   * @post Assert false if the view connection nodes are empty
   * Shutdown the client and then again check for assertion.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testInitAndShutdown() throws IOException, InterruptedException {

    ViewConnection vconn = createViewConn(TestConfig.IPV4_ADDR,8091);
    assertFalse(vconn.getConnectedNodes().isEmpty());
    vconn.shutdown();
    assertTrue(vconn.getConnectedNodes().isEmpty());

  }

  /**
   * Creates a view connection.
   * @param host
   * @param port
   * @return
   * @throws IOException
   */
  private ViewConnection createViewConn(String host, int port) throws IOException {
	CouchbaseConnectionFactory cf = new CouchbaseConnectionFactory(
      Arrays.asList(
        URI.create("http://" + host + ":"+port+"/pools")
      ),
      "default",
      ""
    );

    List<InetSocketAddress> addrs = AddrUtil.getAddressesFromURL(
      cf.getVBucketConfig().getCouchServers()
    );

    ViewConnection vconn = cf.createViewConnection(addrs);
	return vconn;
  }

  /**
   * This test is performed by having the client connect to a host
   * which is up, but using a bad port (i.e. not the default 8091)
   *
   * @pre  First a new instance is created using URI of invalid port.
   * @post  The connection should not succeed, after which the connection
   * nodes are verified to be available or empty.
   * @throws Exception
   */
  @Test
  public void testViewConnRefused() throws IOException, InterruptedException {
    try {
      ViewConnection vconn = createViewConn(TestConfig.IPV4_ADDR,2343);
      assertTrue(vconn.getConnectedNodes().isEmpty());
      vconn.shutdown();
      assertTrue(vconn.getConnectedNodes().isEmpty());
    } catch (Exception e) {
      assertFalse(e.getMessage().isEmpty());
    }
  }

  /**
   * This test is performed by having the client connect
   * to an IP for which no valid host is assigned.
   *
   * @pre  First a new instance is created using URI of invalid host.
   * @post  The connection should not succeed, after which the connection
   * nodes are verified to be available or empty.
   * @throws Exception
   */
  @Test
  public void testNetworkUnreachable() throws IOException,InterruptedException {
    try {
      ViewConnection vconn = createViewConn("10.34.34.23",8091);
      assertTrue(vconn.getConnectedNodes().isEmpty());
      vconn.shutdown();
      assertTrue(vconn.getConnectedNodes().isEmpty());
    } catch (Exception e) {
      assertFalse(e.getMessage().isEmpty());
    }
  }
}