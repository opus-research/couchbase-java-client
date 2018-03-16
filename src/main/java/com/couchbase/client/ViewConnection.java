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
import com.couchbase.client.http.ViewPool;
import com.couchbase.client.protocol.views.HttpOperation;
import com.couchbase.client.vbucket.Reconfigurable;
import com.couchbase.client.vbucket.config.Bucket;
import com.couchbase.client.vbucket.config.DefaultConfig;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import net.spy.memcached.compat.SpyObject;
import org.apache.http.HttpHost;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
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
 * The {@link ViewConnection} is responsible for managing and multiplexing
 * HTTP connections to Couchbase View endpoints.
 *
 * It implements {@link Reconfigurable}, which means that will be fed with
 * reconfiguration updates coming from the server side. This stream changes
 * the collection of {@link HttpHost}s, which represent the view endpoints.
 */
public class ViewConnection extends SpyObject implements Reconfigurable {

  /**
   * The view connection scheme to use for {@link HttpHost}s.
   */
  private static final String SCHEME = "http";

  /**
   * The {@link CouchbaseConnectionFactory} to get more context from.
   */
  private final CouchbaseConnectionFactory connFactory;

  /**
   * The list of view endpoints to communicate with.
   */
  private final List<HttpHost> viewNodes;

  /**
   * The HTTP user to use for authentication.
   */
  private final String user;

  /**
   * The HTTP password to use for authentication.
   */
  private final String password;

  /**
   * The asynchronous IO reactor which executes the requests.
   */
  private final ConnectingIOReactor ioReactor;

  /**
   * The connection pool manager for multiplexing connections.
   */
  private final ViewPool pool;

  /**
   * A requester that helps with asynchronous request/response flow.
   */
  private final HttpAsyncRequester requester;

  /**
   * The thread where the {@link #ioReactor} executes in.
   */
  private volatile Thread reactorThread;

  /**
   * The next node to pick up from {@link #viewNodes}, selected in a round-robin
   * fashion by {@link #getNextNode()}.
   */
  private volatile int nextNode;

  /**
   * If the connection is running or shut down.
   */
  private volatile boolean running;

  /**
   * Boostrap the {@link ViewConnection}s.
   *
   * This will also start the reactor in a separate thread, waiting for requests
   * to be written.
   *
   * @param cf the configuration factory to access other parts of the system.
   * @param addrs the list of nodes to initially connect to.
   * @param user the HTTP username for authentication purposes.
   * @param password the HTTP password for authentication purposes.
   * @throws IOException if the IOReactor could not be created.
   */
  public ViewConnection(final CouchbaseConnectionFactory cf,
    final List<InetSocketAddress> addrs, final String user,
    final String password) throws IOException {
    connFactory = cf;
    nextNode = 0;
    this.user = user;
    this.password = password;

    viewNodes = Collections.synchronizedList(new ArrayList<HttpHost>());
    for (InetSocketAddress addr : addrs) {
      viewNodes.add(createHttpHost(addr.getHostName(), addr.getPort()));
    }

    HttpProcessor httpProc = HttpProcessorBuilder.create()
      .add(new RequestContent())
      .add(new RequestTargetHost())
      .add(new RequestConnControl())
      .add(new RequestUserAgent("JCBC/1.2"))
      .add(new RequestExpectContinue(true)).build();

    requester = new HttpAsyncRequester(httpProc);

    ioReactor = new DefaultConnectingIOReactor(IOReactorConfig.custom()
      .setConnectTimeout(5000)
      .setSoTimeout(5000)
      .setTcpNoDelay(true)
      .setIoThreadCount(cf.getViewWorkerSize())
      .build());

    pool = new ViewPool(ioReactor, ConnectionConfig.DEFAULT);
    pool.setDefaultMaxPerRoute(cf.getViewConnsPerNode());
    updateMaxTotalRequests();

    initializeReactorThread();
  }

  /**
   * Write an operation to the next {@link HttpHost}.
   *
   * To make sure that the operations are distributed throughout the cluster,
   * the {@link HttpHost} is changed every time a new operation is added. Since
   * the {@link #getNextNode()} method increments the {@link HttpHost} index and
   * calculates the modulo, the nodes are selected in a round-robin fashion.
   *
   * @param op the operation to schedule.
   */
  public void addOp(final HttpOperation op) {
    if (!running) {
      throw new IllegalStateException("Shutting down");
    }

    HttpCoreContext coreContext = HttpCoreContext.create();

    if (viewNodes.isEmpty()) {
      getLogger().error("No server connections. Cancelling op.");
      op.cancel();
    } else {
      if (!"default".equals(user)) {
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

      HttpHost httpHost = viewNodes.get(getNextNode());

      requester.execute(
        new BasicAsyncRequestProducer(httpHost, op.getRequest()),
        new BasicAsyncResponseConsumer(),
        pool,
        coreContext,
        new HttpResponseCallback(op, this, httpHost)
      );
    }
  }

  /**
   * Shuts down the active {@link ViewConnection}.
   *
   * @return false if a shutdown attempt is already in progress, true otherwise.
   * @throws IOException if the reactor cannot be shut down properly.
   */
  public boolean shutdown() throws IOException {
    if (!running) {
      getLogger().info("Suppressing duplicate attempt to shut down");
      return false;
    }
    running = false;

    ioReactor.shutdown();
    try {
      reactorThread.join(0);
    } catch (InterruptedException ex) {
      getLogger().error("Interrupt " + ex + " received while waiting for "
        + "view thread to shut down.");
    }
    return true;
  }

  /**
   * Updates the list of active {@link HttpHost}s based on the incoming
   * {@link Bucket} configuration.
   *
   * If the node is in the bucket configuration, but has no active vbuckets
   * assigned to it, it will not be added as an active node (because it will
   * not return useful results without partitions assigned). Also, a node
   * will be removed if during reconfiguration it is determined that all
   * partitions have been movd away from it.
   *
   * @param bucket the updated bucket configuration.
   */
  @Override
  public void reconfigure(final Bucket bucket) {
    int sizeBeforeReconfigure = viewNodes.size();

    List<HttpHost> currentViewServers = new ArrayList<HttpHost>();
    for (URL server : bucket.getConfig().getCouchServers()) {
      HttpHost host = createHttpHost(server.getHost(), server.getPort());
      currentViewServers.add(host);

      if (!viewNodes.contains(host) && hasActiveVBuckets(host)) {
        viewNodes.add(host);
      }
    }

    synchronized (viewNodes) {
      Iterator<HttpHost> iter = viewNodes.iterator();
      while (iter.hasNext()) {
        HttpHost current = iter.next();
        if (!currentViewServers.contains(current)
          || !hasActiveVBuckets(current)) {
          iter.remove();
          pool.closeConnectionsForHost(current);
        }
      }
    }

    if (sizeBeforeReconfigure != viewNodes.size()) {
      updateMaxTotalRequests();
    }
  }

  /**
   * Initialize the reactor IO thread.
   */
  private void initializeReactorThread() {
    final IOEventDispatch ioEventDispatch = new DefaultHttpClientIODispatch(
      new HttpAsyncRequestExecutor(),
      ConnectionConfig.DEFAULT
    );

    reactorThread = new Thread(new Runnable() {
      @Override
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
    reactorThread.start();

    running = true;
  }

  /**
   * Update the absolute maximum of parallel connection requests.
   *
   * The algorithm sets the number of total parallel connection requests to
   * the number of nodes multiplied by the number of connections per node
   * allowed. Note that this number will only be hit under high-pressure
   * scenarios, the reactor itself is optimized to reuse connections if they
   * are idle.
   */
  private void updateMaxTotalRequests() {
    pool.setMaxTotal(viewNodes.size() * pool.getDefaultMaxPerRoute());
  }

  /**
   * Helper method to create {@link HttpHost} instances.
   *
   * @param host the hostname to use.
   * @param port the port to use.
   * @return a {@link HttpHost} instance.
   */
  private static HttpHost createHttpHost(final String host, final int port) {
    return new HttpHost(host, port, SCHEME);
  }

  /**
   * Calculates the next node to run the request against.
   *
   * @return the next index in the {@link #viewNodes} list.
   */
  private int getNextNode() {
    return nextNode = ++nextNode % viewNodes.size();
  }

  /**
   * Check if the given {@link HttpHost} has active partitions assigned.
   *
   * If there are no active VBuckets assigned to the host, there is no need
   * to run the view operations against it, because it cannot serve the
   * incoming request properly.
   *
   * @param node the node to check.
   * @return true if it has active vbuckets, false if not.
   */
  private boolean hasActiveVBuckets(final HttpHost node) {
    DefaultConfig config = (DefaultConfig) connFactory.getVBucketConfig();
    return config.nodeHasActiveVBuckets(
      new InetSocketAddress(node.getHostName(), node.getPort())
    );
  }

  /**
   * Lists the currently serviceable {@link HttpHost}s for verification
   * purposes.
   *
   * @return a list of currently serviceable {@link HttpHost}s.
   */
  List<HttpHost> getConnectedNodes() {
    return viewNodes;
  }

}
