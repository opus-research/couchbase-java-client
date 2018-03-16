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
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
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
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;

/**
 * Establishes a HTTP connection to a single Couchbase node.
 *
 * Based upon http://hc.apache.org/httpcomponents-core-ga/httpcore-nio/
 * examples/org/apache/http/examples/nio/NHttpClientConnManagement.java
 */
public class ViewNode extends SpyObject {

  private final InetSocketAddress addr;
  private final String user;
  private final String pass;
  private boolean shuttingDown = false;

  private final ViewConnection viewConnection;

  private final HttpProcessor httpProc;
  private final ConnectingIOReactor ioReactor;
  private final BasicNIOConnPool pool;
  private final IOEventDispatch ioEventDispatch;
  private final HttpHost host;
  private final HttpAsyncRequester requester;

  private Thread ioThread;

  public ViewNode(InetSocketAddress a, String usr,
    String pwd, ViewConnection conn) throws IOReactorException {
    addr = a;
    user = usr;
    pass = pwd;
    viewConnection = conn;

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

    host = new HttpHost(addr.getHostName(), addr.getPort(), "http");
    requester = new HttpAsyncRequester(httpProc);

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
        getLogger().info("I/O reactor terminated for " + addr.getHostName());
      }
    }, "Couchbase View Thread for node " + addr);
    ioThread.start();
  }

  public boolean writeOp(final HttpOperation op) {
    HttpCoreContext coreContext = HttpCoreContext.create();

    if (!user.equals("default")) {
      try {
        op.addAuthHeader(HttpUtil.buildAuthHeader(user, pass));
      } catch (UnsupportedEncodingException ex) {
        getLogger().error("Could not create auth header for request, "
          + "could not encode credentials into base64. Canceling op."
          + op, ex);
        op.cancel();
        return true;
      }
    }

    requester.execute(
      new BasicAsyncRequestProducer(host, op.getRequest()),
      new BasicAsyncResponseConsumer(),
      pool,
      coreContext,
      new HttpResponseCallback(op, viewConnection)
    );

    return true;
  }

  public InetSocketAddress getSocketAddress() {
    return addr;
  }

  public void shutdown() throws IOException {
    shutdown(0, TimeUnit.MILLISECONDS);
  }

  public void shutdown(long time, TimeUnit unit) throws
    IOException {
    shuttingDown = true;
    long waittime = time;
    if (unit != TimeUnit.MILLISECONDS) {
      waittime = TimeUnit.MILLISECONDS.convert(time, unit);
    }

    ioReactor.shutdown();
    try {
      ioThread.join(waittime);
    } catch (InterruptedException ex) {
      getLogger().error("Interrupt " + ex + " received while waiting for node "
        + addr.getHostName() + " to shut down.");
    }
  }

  public boolean isShuttingDown() {
    return shuttingDown;
  }

}
