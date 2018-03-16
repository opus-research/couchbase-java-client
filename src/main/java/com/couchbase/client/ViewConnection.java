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

import com.couchbase.client.http.HttpResponseCallback;
import com.couchbase.client.http.HttpUtil;
import com.couchbase.client.protocol.views.HttpOperation;
import com.couchbase.client.vbucket.Reconfigurable;
import com.couchbase.client.vbucket.config.Bucket;
import com.couchbase.client.vbucket.config.DefaultConfig;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.compat.SpyObject;
import org.apache.http.HttpHost;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;


/**
 * The ViewConnection class creates and manages the various connections
 * to the ViewNodes.
 */
public class ViewConnection extends SpyObject implements
  Reconfigurable {

  private volatile boolean shutDown;
  protected volatile boolean reconfiguring;
  protected volatile boolean running = true;

  private final CouchbaseConnectionFactory connFactory;

  private int nextNode;

  private List<HttpHost> viewNodes;
  private final String user;
  private final String password;

  private final HttpProcessor httpProc;
  private final ConnectingIOReactor ioReactor;
  private final BasicNIOConnPool pool;
  private final IOEventDispatch ioEventDispatch;
  private final HttpAsyncRequester requester;
  private Thread ioThread;

  /**
   * Kickstarts the initialization and delegates the connection creation.
   *
   * @param cf the factory which contains neeeded information.
   * @param addrs the list of addresses to connect to.
   * @throws IOException
   */
  public ViewConnection(CouchbaseConnectionFactory cf,
      List<InetSocketAddress> addrs, String user, String password)
    throws IOException {
    connFactory = cf;
    viewNodes = Collections.synchronizedList(createConnections(addrs));
    nextNode = 0;
    this.user = user;
    this.password = password;

    httpProc = HttpProcessorBuilder.create()
      .add(new RequestContent())
      .add(new RequestTargetHost())
      .add(new RequestConnControl())
      .add(new RequestUserAgent("JCBC/1.2"))
      .add(new RequestExpectContinue(true)).build();

    // Create client-side HTTP protocol handler
    HttpAsyncRequestExecutor protocolHandler = new HttpAsyncRequestExecutor();

    // Create client-side I/O event dispatch
    ioEventDispatch = new DefaultHttpClientIODispatch(protocolHandler,
      ConnectionConfig.DEFAULT);

    // Create client-side I/O reactor
    ioReactor = new DefaultConnectingIOReactor(IOReactorConfig.custom()
      .setConnectTimeout(5000)
      .setSoTimeout(5000)
      .setTcpNoDelay(true)
      .build());

    // Create HTTP connection pool
    pool = new BasicNIOConnPool(ioReactor, ConnectionConfig.DEFAULT);

    // Limit total number of connections to just two
    pool.setDefaultMaxPerRoute(2);
    pool.setMaxTotal(2);

    requester = new HttpAsyncRequester(httpProc);

    init();
  }

  public void init() {
    // Start the I/O reactor in a separate thread
    ioThread = new Thread(new Runnable() {
      public void run() {
        try {
          ioReactor.execute(ioEventDispatch);
        } catch (InterruptedIOException ex) {
          getLogger().error("I/O reactor Interrupted", ex);
        } catch (IOException e) {
          getLogger().error("I/O error: " + e.getMessage(), e);
        }
        getLogger().info("I/O reactor terminated for ");
      }
    }, "Couchbase View Thread");
    ioThread.start();
  }

  /**
   * Create ViewNode connections and queue them up for connect.
   *
   * This method also defines the connection params for each connection,
   * including the default settings like timeouts and the user agent string.
   *
   * @param addrs addresses of all the nodes it should connect to.
   * @return Returns a list of the ViewNodes.
   * @throws IOException
   */
  private List<HttpHost> createConnections(List<InetSocketAddress> addrs) {

    List<HttpHost> nodeList = new LinkedList<HttpHost>();

    for (InetSocketAddress addr : addrs) {
      nodeList.add(new HttpHost(addr.getHostName(), addr.getPort(), "http"));
    }

    return nodeList;
  }

  /**
   * Write an operation to the next ViewNode.
   *
   * To make sure that the operations are distributed throughout the cluster,
   * the ViewNode is changed every time a new operation is added. Since the
   * getNextNode() method increments the ViewNode IDs and calculates the
   * modulo, the nodes are selected in a round-robin fashion.
   *
   * @param op the operation to run.
   */
  public void addOp(final HttpOperation op) {
    HttpCoreContext coreContext = HttpCoreContext.create();

    if (viewNodes.isEmpty()) {
      getLogger().error("No server connections. Cancelling op.");
      op.cancel();
    } else {
      if (!user.equals("default")) {
        try {
          op.addAuthHeader(HttpUtil.buildAuthHeader(user, password));
        } catch (UnsupportedEncodingException ex) {
          getLogger().error("Could not create auth header for request, "
            + "could not encode credentials into base64. Canceling op."
            + op, ex);
          op.cancel();
          return;
        }
      }

      HttpHost httpHost;
      while (true) {
        httpHost = viewNodes.get(getNextNode());
        if (hasActiveVBuckets(httpHost)) {
          break;
        }
      }

      requester.execute(
        new BasicAsyncRequestProducer(httpHost, op.getRequest()),
        new BasicAsyncResponseConsumer(),
        pool,
        coreContext,
        new HttpResponseCallback(op, this)
      );
    }
  }

  /**
   * Calculates the next node to run the operation on.
   *
   * @return id of the next node.
   */
  private int getNextNode() {
    return nextNode = (++nextNode % viewNodes.size());
  }

  /**
   * Check if the a http host has active VBuckets.
   *
   * @param node the node to check
   * @return true or false if it has active VBuckets.
   */
  private boolean hasActiveVBuckets(HttpHost node) {
    DefaultConfig config = (DefaultConfig) connFactory.getVBucketConfig();
    return config.nodeHasActiveVBuckets(new InetSocketAddress(node.getHostName(), node.getPort()));
  }

  /**
   * Returns the currently connected ViewNodes.
   *
   * @return a list of currently connected ViewNodes.
   */
  public List<HttpHost> getConnectedNodes() {
    return viewNodes;
  }

  /**
   * Checks the state of the ViewConnection.
   *
   * If shutdown is currently in progress, an Exception is thrown.
   */
  protected void checkState() {
    if (shutDown) {
      throw new IllegalStateException("Shutting down");
    }
  }

  /**
   * Initiates the shutdown of all connected ViewNodes.
   *
   * @return false if a connection is already in progress, true otherwise.
   * @throws IOException
   */
  public boolean shutdown() throws IOException {
    if (shutDown) {
      getLogger().info("Suppressing duplicate attempt to shut down");
      return false;
    }

    shutDown = true;
    running = false;

    ioReactor.shutdown();
    try {
      ioThread.join(0);
    } catch (InterruptedException ex) {
      getLogger().error("Interrupt " + ex + " received while waiting for "
        + "view thread to shut down.");
    }
    return true;
  }

  /**
   * Reconfigures the connected ViewNodes.
   *
   * When a reconfiguration event happens, new ViewNodes may need to be added
   * or old ones need to be removed from the current configuration. This method
   * takes care that those operations are performed in the correct order and
   * are executed in a thread-safe manner.
   *
   * @param bucket the bucket which has been rebalanced.
   */
  public void reconfigure(Bucket bucket) {
    reconfiguring = true;

    HashSet<SocketAddress> newServerAddresses = new HashSet<SocketAddress>();
    List<InetSocketAddress> newServers = AddrUtil.getAddressesFromURL(
      bucket.getConfig().getCouchServers());
    for (InetSocketAddress server : newServers) {
      newServerAddresses.add(server);
    }

    ArrayList<InetSocketAddress> stayServers = new ArrayList<InetSocketAddress>();

    try {
      synchronized (viewNodes) {
        Iterator<HttpHost> iter = viewNodes.iterator();
        while (iter.hasNext()) {
          HttpHost current = iter.next();
          if (!newServerAddresses.contains(current.getAddress())) {
            iter.remove();
          }
        }
      }

      // prepare a collection of addresses for new nodes
      newServers.removeAll(stayServers);

      // create a collection of new nodes
      List<HttpHost> newNodes = createConnections(newServers);
      viewNodes.addAll(newNodes);
    } finally {
      reconfiguring = false;
    }
  }
}
