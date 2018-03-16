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

package com.couchbase.client.vbucket.provider;

import com.couchbase.client.CouchbaseConnection;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.vbucket.ConfigurationException;
import com.couchbase.client.vbucket.ConfigurationProviderHTTP;
import com.couchbase.client.vbucket.CouchbaseNodeOrder;
import com.couchbase.client.vbucket.Reconfigurable;
import com.couchbase.client.vbucket.config.Bucket;
import com.couchbase.client.vbucket.config.Config;
import com.couchbase.client.vbucket.config.ConfigurationParser;
import com.couchbase.client.vbucket.config.ConfigurationParserJSON;
import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.auth.AuthThreadMonitor;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This {@link ConfigurationProvider} provides the current bucket configuration
 * in a best-effort way, mixing both http and binary fetching techniques
 * (depending on the supported mechanisms on the cluster side).
 */
public class BucketConfigurationProvider extends SpyObject
  implements ConfigurationProvider, Reconfigurable {

  private static final int DEFAULT_BINARY_PORT = 11210;

  private final AtomicReference<Bucket> config;
  private final List<URI> seedNodes;
  private final String bucket;
  private final String password;
  private final CouchbaseConnectionFactory connectionFactory;
  private final ConfigurationParser configurationParser;
  private final AtomicReference<ConfigurationProviderHTTP> httpProvider;
  private final AtomicBoolean refreshingHttp;
  private volatile boolean isBinary;

  public BucketConfigurationProvider(final List<URI> seedNodes,
    final String bucket, final String password,
    final CouchbaseConnectionFactory connectionFactory) {
    config = new AtomicReference<Bucket>();
    configurationParser = new ConfigurationParserJSON();
    httpProvider = new AtomicReference<ConfigurationProviderHTTP>(
      new ConfigurationProviderHTTP(seedNodes, bucket, password)
    );
    refreshingHttp = new AtomicBoolean(false);

    this.seedNodes = new ArrayList<URI>(seedNodes);
    this.bucket = bucket;
    this.password = password;
    this.connectionFactory = connectionFactory;
  }

  @Override
  public Bucket bootstrap() throws ConfigurationException {
    if (!bootstrapBinary() && !bootstrapHttp()) {
      throw new ConfigurationException("Could not fetch a valid Bucket "
        + "configuration.");
    }

    monitorBucket();
    return config.get();
  }

  /**
   * Helper method to initiate the binary bootstrap process.
   *
   * If no config is found (either because of an error or it is not supported
   * on the cluster side), false is returned.
   *
   * @return true if the binary bootstrap process was successful.
   */
  boolean bootstrapBinary() {
    CouchbaseConnectionFactory cf = connectionFactory;
    List<InetSocketAddress> nodes =
      new ArrayList<InetSocketAddress>(seedNodes.size());
    for (URI seedNode : seedNodes) {
      nodes.add(new InetSocketAddress(seedNode.getHost(), DEFAULT_BINARY_PORT));
    }

    CouchbaseConnection connection = null;
    try {
      connection = new CouchbaseConnection(
        cf.getReadBufSize(), cf, nodes, cf.getInitialObservers(),
        cf.getFailureMode(), cf.getOperationFactory()
      );

      AuthThreadMonitor monitor = new AuthThreadMonitor();
      List<MemcachedNode> connectedNodes = new ArrayList<MemcachedNode>(
        connection.getLocator().getAll());
      for (MemcachedNode node : connectedNodes) {
        monitor.authConnection(connection, cf.getOperationFactory(),
          cf.getAuthDescriptor(), node);
      }

      final List<String> configs = Collections.synchronizedList(
        new ArrayList<String>());
      CountDownLatch blatch = connection.broadcastOperation(
        new BroadcastOpFactory() {
          @Override
          public Operation newOp(MemcachedNode n, final CountDownLatch latch) {
            return new GetConfigOperationImpl(new OperationCallback() {
              @Override
              public void receivedStatus(OperationStatus status) {
                if (status.isSuccess()) {
                  configs.add(status.getMessage());
                }
              }

              @Override
              public void complete() {
                latch.countDown();
              }
            });
          }
        }
      );
      blatch.await(cf.getOperationTimeout(), TimeUnit.MILLISECONDS);

      if (configs.isEmpty()) {
        getLogger().debug("Could not load a single config over binary.");
        return false;
      }

      String appliedConfig = connection.replaceConfigWildcards(configs.get(0));
      Bucket config = configurationParser.parseBucket(appliedConfig);
      setConfig(config);
      isBinary = true;
      return true;
    } catch(Exception ex) {
      getLogger().info("Could not fetch config from binary seed nodes.", ex);
      return false;
    } finally {
      if (connection != null) {
        try {
          connection.shutdown();
        } catch (IOException ex) {
          getLogger().info("Could not shutdown the connection", ex);
        }
      }
    }
  }

  /**
   * Helper method to initiate the http bootstrap process.
   *
   * If no config is found (because of an error), false is returned. For now,
   * this is delegated to the old HTTP provider, but no monitor is attached
   * for a subsequent streaming connection.
   *
   * @return true if the http bootstrap process was successful.
   */
  boolean bootstrapHttp() {
    try {
      Bucket config = httpProvider.get().getBucketConfiguration(bucket);
      setConfig(config);
      return true;
    } catch(Exception ex) {
      getLogger().info("Could not fetch config from http seed nodes.", ex);
      return false;
    }
  }

  /**
   * Start to monitor the bucket configuration, depending on the provider
   * used.
   */
  private void monitorBucket() {
    if (!isBinary) {
      httpProvider.get().subscribe(bucket, this);
    }
  }

  @Override
  public void reconfigure(final Bucket bucket) {
    setConfig(bucket);
  }

  @Override
  public Bucket getConfig() {
    return config.get();
  }

  @Override
  public void setConfig(final Bucket config) {
    this.config.set(config);
    updateSeedNodes();
  }

  /**
   * Updates the current list of seed nodes with a current one from the stored
   * configuration.
   */
  private void updateSeedNodes() {
    Config config = this.config.get().getConfig();

    List<String> clusterNodes = config.getRestEndpoints();
    if (clusterNodes.size() > 0) {
      List<URI> newNodes = new ArrayList<URI>();
      for (String clusterNode : clusterNodes) {
        try {
          newNodes.add(new URI(clusterNode));
        } catch(URISyntaxException ex) {
          getLogger().warn("Could not add node to updated bucket list because "
            + "of a parsing exception.");
          getLogger().debug("Could not parse list because: " + ex);
        }
      }

      if (seedNodesAreDifferent(seedNodes, newNodes)) {
        potentiallyRandomizeNodeList(newNodes);
        seedNodes.clear();
        seedNodes.addAll(newNodes);
        httpProvider.get().updateBaseListFromConfig(seedNodes);
      }
    }
  }

  private void potentiallyRandomizeNodeList(List<URI> list) {
    if (connectionFactory.getStreamingNodeOrder() == CouchbaseNodeOrder.ORDERED) {
      return;
    }
    Collections.shuffle(list);
  }

  private static boolean seedNodesAreDifferent(List<URI> left, List<URI> right) {
    if (left.size() != right.size()) {
      return true;
    }

    for (URI uri : left) {
      if (!right.contains(uri)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void signalOutdated() {
    if (isBinary) {
      // try to fetch new configuration over binary connection
    } else {
      if (refreshingHttp.compareAndSet(false, true)) {
        Thread refresherThread = new Thread(new HttpProviderRefresher(this));
        refresherThread.setName("HttpConfigurationProvider Reloader");
        refresherThread.run();
      }
    }
  }

  @Override
  public void shutdown() {
    httpProvider.get().shutdown();
  }

  class HttpProviderRefresher implements Runnable {

    private final BucketConfigurationProvider provider;

    public HttpProviderRefresher(BucketConfigurationProvider provider) {
      this.provider = provider;
    }

    @Override
    public void run() {
      try {
        ConfigurationProviderHTTP oldProvider = httpProvider.get();
        ConfigurationProviderHTTP newProvider = new ConfigurationProviderHTTP(
          seedNodes, bucket, password);
        newProvider.subscribe(bucket, provider);
        httpProvider.set(newProvider);
        oldProvider.shutdown();
      } finally {
        refreshingHttp.set(false);
      }
    }
  }

}
