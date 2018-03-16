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

import com.couchbase.client.internal.HttpCompletionListener;
import com.couchbase.client.internal.HttpFuture;
import com.couchbase.client.protocol.views.HttpOperation;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.QueryTest;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewFetcherOperation;
import com.couchbase.client.protocol.views.ViewFetcherOperationImpl;
import com.couchbase.client.protocol.views.ViewResponse;
import net.spy.memcached.TestConfig;
import net.spy.memcached.ops.OperationStatus;
import org.apache.http.HttpRequest;
import org.apache.http.HttpVersion;
import org.apache.http.message.BasicHttpRequest;
import org.jboss.netty.channel.Channel;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the {@link NettyViewConnection}.
 */
public class NettyViewConnectionTest {

  private static final int VIEW_PORT = 8092;
  private static final ExecutorService EXEC = Executors.newCachedThreadPool();

  private static final List<InetSocketAddress> validNodes =
    new ArrayList<InetSocketAddress>();

  private static final List<InetSocketAddress> invalidNodes =
    new ArrayList<InetSocketAddress>();


  @BeforeClass
  public static void setupConnectAddrs() {
    validNodes.add(new InetSocketAddress(TestConfig.IPV4_ADDR, VIEW_PORT));
    invalidNodes.add(new InetSocketAddress("foobarHost", VIEW_PORT));
  }

  @Test
  public void shouldBootstrapOnInit() {
    NettyViewConnection conn = new NettyViewConnection(validNodes);
    List<Channel> connected = conn.getConnectedChannels();

    assertTrue(connected.size() > 0);
    assertEquals(validNodes.size(), connected.size());

    for (Channel channel : connected) {
      InetSocketAddress remote = (InetSocketAddress) channel.getRemoteAddress();
      assertNotNull(channel.getLocalAddress());
      assertNotNull(remote);
      assertEquals(VIEW_PORT, remote.getPort());
    }
  }

  @Test
  public void shouldFailOnInvalidNodeOnBootstrap() {
    try {
      new NettyViewConnection(invalidNodes);
      assertTrue("Exception not thrown", false);
    } catch (RuntimeException ex) {
      assertEquals("Could not connect to all nodes on bootstrap.",
        ex.getMessage());
    } catch (Exception ex) {
      assertTrue("Wrong exception thrown", false);
    }
  }

  @Test
  public void perf() throws Exception {
    CouchbaseClient client = new CouchbaseClient(Arrays.asList(new URI("http://127.0.0.1:8091/pools")), "beer-sample", "");

    View view = client.getView("beer", "brewery_beers");
    Query query = new Query();
    query.setStale(Stale.OK).setLimit(1);

    final int RUNS = 10000;
    final CountDownLatch l = new CountDownLatch(RUNS);
    long start = System.currentTimeMillis();
    final AtomicInteger checkCounter = new AtomicInteger(0);
    for (int i = 0; i < RUNS; i++) {
      client.asyncQuery(view, query).addListener(new HttpCompletionListener() {
        @Override
        public void onComplete(HttpFuture<?> httpFuture) throws Exception {
          l.countDown();
          checkCounter.incrementAndGet();
        }
      });
    }
    l.await();
    long end = System.currentTimeMillis();

    System.out.println(end - start);
    System.out.println(checkCounter.get() == RUNS);

    //client.shutdown();
  }

}
