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

import com.couchbase.client.clustermanager.AuthType;
import com.couchbase.client.clustermanager.BucketType;
import com.couchbase.client.clustermanager.FlushResponse;
import net.spy.memcached.compat.SpyObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
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
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A client to perform cluster-wide operations over the HTTP REST API.
 */
public class ClusterManager extends SpyObject {

  /**
   * The list of cluster nodes to communicate with.
   */
  private final List<HttpHost> clusterNodes;

  /**
   * The asynchronous IO reactor which executes the requests.
   */
  private final ConnectingIOReactor ioReactor;

  /**
   * The connection pool manager for multiplexing connections.
   */
  private final BasicNIOConnPool pool;

  /**
   * A requester that helps with asynchronous request/response flow.
   */
  private final HttpAsyncRequester requester;

  /**
   * The REST API (admin) username.
   */
  private final String username;

  /**
   * The REST API (admin) password.
   */
  private final String password;

  /**
   * The thread where the {@link #ioReactor} executes in.
   */
  private volatile Thread reactorThread;

  /**
   * If the connection is running or shut down.
   */
  private volatile boolean running;

  /**
   * Create a new {@link ClusterManager} instance.
   *
   * @param nodes the list of nodes in the cluster.
   * @param username the admin username.
   * @param password the admin password.
   */
  public ClusterManager(final List<URI> nodes, final String username,
    final String password) {
    if (nodes == null || nodes.isEmpty()) {
      throw new IllegalArgumentException("List of nodes is null or empty");
    }
    if (username == null || username.isEmpty()) {
      throw new IllegalArgumentException("Username is null or empty");
    }
    if (password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Password is null or empty");
    }

    this.username = username;
    this.password = password;

    clusterNodes = Collections.synchronizedList(new ArrayList<HttpHost>());
    for (URI node : nodes) {
      clusterNodes.add(new HttpHost(node.getHost(), node.getPort()));
    }

    HttpProcessor httpProc = HttpProcessorBuilder.create()
      .add(new RequestContent())
      .add(new RequestTargetHost())
      .add(new RequestConnControl())
      .add(new RequestUserAgent("JCBC/1.2"))
      .add(new RequestExpectContinue(true)).build();

    requester = new HttpAsyncRequester(httpProc);

    try {
    ioReactor = new DefaultConnectingIOReactor(IOReactorConfig.custom()
      .setConnectTimeout(5000)
      .setSoTimeout(5000)
      .setTcpNoDelay(true)
      .setIoThreadCount(1)
      .build());
    } catch (IOReactorException ex) {
      throw new IllegalStateException("Could not create IO reactor");
    }

    pool = new BasicNIOConnPool(ioReactor, ConnectionConfig.DEFAULT);
    pool.setDefaultMaxPerRoute(5);
    initializeReactorThread();
  }

  /**
   * Creates the default bucket.
   *
   * @param type The bucket type to create.
   * @param memorySizeMB The amount of memory to allocate to this bucket.
   * @param replicas The number of replicas for this bucket.
   * @param flushEnabled If flush should be enabled on this bucket.
   */
  public void createDefaultBucket(BucketType type, int memorySizeMB,
    int replicas, boolean flushEnabled) {
    createBucket(type, "default", memorySizeMB, AuthType.NONE, replicas,
      11212, "", flushEnabled);
  }

  /**
   * Creates a named bucket with a given password for SASL authentication.
   *
   * @param type The bucket type to create.
   * @param name The name of the bucket.
   * @param memorySizeMB The amount of memory to allocate to this bucket.
   * @param replicas The number of replicas for this bucket.
   * @param authPassword The password for this bucket.
   * @param flushEnabled If flush should be enabled on this bucket.
   */
  public void createNamedBucket(BucketType type, String name,
    int memorySizeMB, int replicas, String authPassword,
    boolean flushEnabled) {
    createBucket(type, name, memorySizeMB, AuthType.SASL, replicas,
      11212, authPassword, flushEnabled);
  }

  /**
   * Creates the a sasl bucket.
   *
   * @param type The bucket type to create.
   * @param name The name of the bucket.
   * @param memorySizeMB The amount of memory to allocate to this bucket.
   * @param replicas The number of replicas for this bucket.
   * @param port The port for this bucket to listen on.
   */
  public void createPortBucket(BucketType type, String name,
    int memorySizeMB, int replicas, int port, boolean flush) {
    createBucket(type, name, memorySizeMB, AuthType.NONE, replicas,
      port, "", flush);
  }

  /**
   * Deletes a bucket.
   *
   * @param name The name of the bucket to delete.
   */
  public void deleteBucket(String name) {
    String url = "/pools/default/buckets/" + name;
    BasicHttpEntityEnclosingRequest request =
      new BasicHttpEntityEnclosingRequest("DELETE", url);

    checkError(200, sendRequest(request));
  }

  /**
   * Lists all buckets in a Couchbase cluster.
   */
  public List<String> listBuckets() {
    String url = "/pools/default/buckets/";
    BasicHttpEntityEnclosingRequest request =
      new BasicHttpEntityEnclosingRequest("GET", url);

    HttpResult result = sendRequest(request);
    checkError(200, result);

    String json = result.getBody();
    List<String> names = new LinkedList<String>();
    if (json != null && !json.isEmpty()) {
      try {
        JSONArray base = new JSONArray(json);
        for (int i = 0; i < base.length(); i++) {
          JSONObject bucket = (JSONObject) base.get(i);
          if (bucket.has("name")) {
            names.add(bucket.getString("name"));
          }
        }
      } catch (JSONException e) {
        getLogger().error("Unable to interpret list buckets response.");
        throw new RuntimeException(e);
      }
    }
    return names;
  }

  /**
   * Deletes all data in a bucket.
   *
   * @param name The bucket to flush.
   */
  public FlushResponse flushBucket(String name) {
    String url = "/pools/default/buckets/" + name + "/controller/doFlush";
    BasicHttpEntityEnclosingRequest request =
      new BasicHttpEntityEnclosingRequest("POST", url);

    HttpResult result = sendRequest(request);
    if(result.getErrorCode() == 200) {
      return FlushResponse.OK;
    } else if(result.getErrorCode() == 400) {
      return FlushResponse.NOT_ENABLED;
    } else {
      throw new RuntimeException("Http Error: " + result.getErrorCode()
        + " Reason: " + result.getErrorPhrase() + " Details: "
        + result.getReason());
    }

  }

  public void updateBucket(String name, int memorySizeMB,
    AuthType authType, int replicas, int port,
    String authpassword, boolean flushEnabled) {

    try {
      BasicHttpEntityEnclosingRequest request =
        new BasicHttpEntityEnclosingRequest("POST", "/pools/default/buckets/"+name);

      StringBuilder sb = new StringBuilder();
      sb.append("&ramQuotaMB=").append(memorySizeMB);
      sb.append("&authType=").append(authType.getAuthType());
      sb.append("&replicaNumber=").append(replicas);
      sb.append("&proxyPort=").append(port);
      if (authType == AuthType.SASL) {
        sb.append("&saslPassword=").append(authpassword);
      }
      if(flushEnabled) {
        sb.append("&flushEnabled=1");
      }

      request.setEntity(new StringEntity(sb.toString()));
      checkError(200, sendRequest(request));
    } catch (UnsupportedEncodingException e) {
      getLogger().error("Error creating request. Bad arguments");
      throw new RuntimeException(e);
    }
  }

  private  void createBucket(BucketType type, String name,
    int memorySizeMB, AuthType authType, int replicas, int port,
    String authpassword, boolean flushEnabled) {

    List<String> buckets = listBuckets();
    if(buckets.contains(name)){
      throw new RuntimeException("Bucket with given name already exists");
    } else {
      BasicHttpEntityEnclosingRequest request =
        new BasicHttpEntityEnclosingRequest("POST", "/pools/default/buckets");

      StringBuilder sb = new StringBuilder();
      sb.append("name=").append(name);
      sb.append("&ramQuotaMB=").append(memorySizeMB);
      sb.append("&authType=").append(authType.getAuthType());
      sb.append("&replicaNumber=").append(replicas);
      sb.append("&bucketType=").append(type.getBucketType());
      sb.append("&proxyPort=").append(port);
      if (authType == AuthType.SASL) {
        sb.append("&saslPassword=").append(authpassword);
      }
      if(flushEnabled) {
        sb.append("&flushEnabled=1");
      }

      try {
        request.setEntity(new StringEntity(sb.toString()));
      } catch (UnsupportedEncodingException e) {
        getLogger().error("Error creating request. Bad arguments");
        throw new RuntimeException(e);
      }

      checkError(202, sendRequest(request));
    }
  }

  private HttpResult sendRequest(HttpRequest request) {
    HttpCoreContext coreContext = HttpCoreContext.create();

    request.addHeader("Authorization", "Basic "
      + Base64.encodeBase64String((username + ':' + password).getBytes()));
    request.addHeader("Accept", "*/*");
    request.addHeader("Content-Type", "application/x-www-form-urlencoded");

    for (HttpHost node : clusterNodes) {
      try {

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(false);
        final AtomicReference<HttpResponse> response =
          new AtomicReference<HttpResponse>();

        requester.execute(
          new BasicAsyncRequestProducer(node, request),
          new BasicAsyncResponseConsumer(),
          pool,
          coreContext,
          new FutureCallback<HttpResponse>() {
            @Override
            public void completed(final HttpResponse result) {
              latch.countDown();
              success.set(true);
              response.set(result);
            }

            @Override
            public void failed(Exception ex) {
              getLogger().debug("Cluster Response failed with: ", ex);
              latch.countDown();
            }

            @Override
            public void cancelled() {
              getLogger().debug("Cluster Response was cancelled.");
              latch.countDown();
            }
          }
        );

        latch.await();
        if (!success.get()) {
          getLogger().debug("Could not finish request execution");
          continue;
        }

        int code = response.get().getStatusLine().getStatusCode();
        String body = EntityUtils.toString(response.get().getEntity());
        String reason = parseError(body);
        String phrase = response.get().getStatusLine().getReasonPhrase();
        return new HttpResult(body, code, phrase, reason);
      } catch (InterruptedException ex) {
        getLogger().debug("Got interrupted while waiting for the response.");
        continue;
      } catch (IOException e) {
        getLogger().debug("Unable to connect to: " + node
          + ". Trying another server");
      }
    }
    throw new RuntimeException("Unable to connect to cluster");
  }

  private static String parseError(String json) {
    if (json != null && !json.isEmpty()) {
      try {
        JSONObject base = new JSONObject(json);
        if (base.has("errors")) {
          JSONObject errors = (JSONObject) base.get("errors");
          return errors.toString();
        }
      } catch (JSONException e) {
        return "Client error parsing error response";
      }
    }
    return "No reason given";
  }

  private static void checkError(int expectedCode, HttpResult result)  {
    if (result.getErrorCode() != expectedCode) {
      throw new RuntimeException("Http Error: " + result.getErrorCode()
        + " Reason: " + result.getErrorPhrase() + " Details: "
        + result.getReason());
    }
  }
  public boolean shutdown() {
    if (!running) {
      getLogger().info("Suppressing duplicate attempt to shut down");
      return false;
    }
    running = false;

    try {
      ioReactor.shutdown();
    } catch (IOException e) {
      getLogger().info("Got exception while shutting down", e);
    }
    try {
      reactorThread.join(0);
    } catch (InterruptedException ex) {
      getLogger().error("Interrupt " + ex + " received while waiting for "
        + "view thread to shut down.");
    }
    return true;
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
        getLogger().debug("I/O reactor terminated");
      }
    }, "Couchbase View Thread");
    reactorThread.start();

    running = true;
  }

  public final static class HttpResult {
    private final String body;
    private final int errorCode;
    private final String errorPhrase;
    private final String errorReason;

    public HttpResult(String entity, int code, String phrase, String reason) {
      body = entity;
      errorCode = code;
      errorPhrase = phrase;
      errorReason = reason;
    }

    public String getBody() {
      return body;
    }

    public int getErrorCode() {
      return errorCode;
    }

    public String getErrorPhrase() {
      return errorPhrase;
    }

    public String getReason() {
      return errorReason;
    }
  }
}
