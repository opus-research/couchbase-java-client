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
import net.spy.memcached.ArrayModNodeLocator;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.KetamaNodeLocator;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
  private static final String ANONYMOUS_BUCKET = "default";

  private final AtomicReference<Bucket> config;
  private final List<URI> seedNodes;
  private final List<Reconfigurable> observers;
  private final String bucket;
  private final String password;
  private final CouchbaseConnectionFactory connectionFactory;
  private final ConfigurationParser configurationParser;
  private final AtomicReference<ConfigurationProviderHTTP> httpProvider;
  private final AtomicBoolean refreshingHttp;
  private final AtomicReference<CouchbaseConnection> binaryConnection;
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
    observers = Collections.synchronizedList(new ArrayList<Reconfigurable>());
    binaryConnection = new AtomicReference<CouchbaseConnection>();

    this.seedNodes = new ArrayList<URI>(seedNodes);
    this.bucket = bucket;
    this.password = password;
    this.connectionFactory = connectionFactory;
    potentiallyRandomizeNodeList(seedNodes);
  }

  @Override
  public Bucket bootstrap() throws ConfigurationException {
    isBinary = false;
    if (!bootstrapBinary() && !bootstrapHttp()) {
      throw new ConfigurationException("Could not fetch a valid Bucket "
        + "configuration.");
    }

    getLogger().debug("Could bootstrap from binary: " + isBinary);

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
      for (InetSocketAddress node : nodes) {
        ConfigurationConnectionFactory fact = new ConfigurationConnectionFactory(
          seedNodes, bucket, password);
        try {
          connection = new CouchbaseConnection(
            cf.getReadBufSize(), fact, Arrays.asList(node), cf.getInitialObservers(),
            cf.getFailureMode(), cf.getOperationFactory()
          );
        } catch (Exception ex) {
          getLogger().debug("Could not load binary config from "
            + node.getHostName() + ", trying next node.", ex);
          continue;
        }

        if (!bucket.equals(ANONYMOUS_BUCKET)) {
          AuthThreadMonitor monitor = new AuthThreadMonitor();
          List<MemcachedNode> connectedNodes = new ArrayList<MemcachedNode>(
            connection.getLocator().getAll());
          for (MemcachedNode connectedNode : connectedNodes) {
            monitor.authConnection(connection, cf.getOperationFactory(),
              cf.getAuthDescriptor(), connectedNode);
          }
        }

        List<String> configs = getConfigsFromBinaryConnection(connection);

        if (configs.isEmpty()) {
          getLogger().debug("Could not load binary config from "
            + node.getHostName() + ", trying next node.");
          continue;
        }

        String appliedConfig = connection.replaceConfigWildcards(configs.get(0));
        Bucket config = configurationParser.parseBucket(appliedConfig);
        setConfig(config);
        binaryConnection.set(connection);
        isBinary = true;
        return true;
      }

      getLogger().debug("Not a single node returned a binary config.");
      return false;
    } catch(Exception ex) {
      getLogger().info("Could not fetch config from binary seed nodes.", ex);
      return false;
    }
  }

  /**
   * Load the configs from a binary connection through a broadcast op.
   *
   * Note that this operation is blocking, so run in a thread pool if needed.
   *
   * @param connection the connection to execute against.
   * @return a list of configs (potentially empty, but not null)
   */
  private List<String> getConfigsFromBinaryConnection(
    final CouchbaseConnection connection) throws Exception {
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

    blatch.await(connectionFactory.getOperationTimeout(), TimeUnit.MILLISECONDS);
    return configs;
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
    if (config.get() == null) {
      bootstrap();
    }
    return config.get();
  }

  @Override
  public void setConfig(final Bucket config) {
    getLogger().debug("Applying new bucket config for bucket \"" + bucket
      + "\": " + config);

    this.config.set(config);
    updateSeedNodes();
    if (config.isNotUpdating()) {
      signalOutdated();
    }
    notifyObservers();
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
      if (binaryConnection.get() == null) {
        bootstrap();
      } else {
        try {
          List<String> configs = getConfigsFromBinaryConnection(
            binaryConnection.get());
          String appliedConfig = binaryConnection.get().replaceConfigWildcards(
            configs.get(0));
          Bucket config = configurationParser.parseBucket(appliedConfig);
          setConfig(config);
        } catch(Exception ex) {
          getLogger().info("Could not load binary config from existing "
            + "connection, rerunning bootstrap.", ex);
          bootstrap();
        }
      }
    } else {
      if (refreshingHttp.compareAndSet(false, true)) {
        Thread refresherThread = new Thread(new HttpProviderRefresher(this));
        refresherThread.setName("HttpConfigurationProvider Reloader");
        refresherThread.run();
      } else {
        getLogger().debug("Suppressing duplicate refreshing attempt.");
      }
    }
  }

  @Override
  public void shutdown() {
    if (httpProvider.get() != null) {
      httpProvider.get().shutdown();
    }
    if (binaryConnection.get() != null) {
      try {
        binaryConnection.get().shutdown();
      } catch (IOException e) {
        getLogger().warn("Could not shutdown config binary connection.");
      }
    }
  }

  @Override
  public String getAnonymousAuthBucket() {
    return ANONYMOUS_BUCKET;
  }

  @Override
  public void setConfig(final String config) {
    try {
      setConfig(configurationParser.parseBucket(config));
    } catch (Exception ex) {
      getLogger().warn("Got new config to update, but could not decode it. "
        + "Staying with old one.", ex);
    }
  }

  @Override
  public void subscribe(Reconfigurable rec) {
    observers.add(rec);
  }

  @Override
  public void unsubscribe(Reconfigurable rec) {
    observers.remove(rec);
  }

  private void notifyObservers() {
    synchronized (observers) {
      Iterator<Reconfigurable> i = observers.iterator();
      while (i.hasNext()) {
        Reconfigurable rec = i.next();
        getLogger().debug("Notifying Observer of new configuration: "
          + rec.getClass().getSimpleName());
        rec.reconfigure(getConfig());
      }
    }
  }

  class HttpProviderRefresher implements Runnable {

    private final BucketConfigurationProvider provider;

    public HttpProviderRefresher(BucketConfigurationProvider provider) {
      this.provider = provider;
    }

    @Override
    public void run() {
      try {

        long reconnectAttempt = 0;
        long backoffTime = 1000;
        long maxWaitTime = 10000;
        while(true) {
          try {
            long waitTime = reconnectAttempt++ * backoffTime;
            if(reconnectAttempt >= 10) {
              waitTime = maxWaitTime;
            }
            getLogger().info("Reconnect attempt " + reconnectAttempt
              + ", waiting " + waitTime + "ms");
            Thread.sleep(waitTime);

            ConfigurationProviderHTTP oldProvider = httpProvider.get();
            ConfigurationProviderHTTP newProvider = new ConfigurationProviderHTTP(
              seedNodes, bucket, password);
            newProvider.subscribe(bucket, provider);
            httpProvider.set(newProvider);
            oldProvider.shutdown();
            return;
          } catch(Exception ex) {
            continue;
          }
        }
      } finally {
        refreshingHttp.set(false);
      }
    }
  }

  static class ConfigurationConnectionFactory extends CouchbaseConnectionFactory {
    ConfigurationConnectionFactory(List<URI> baseList, String bucketName, String password) throws IOException {
      super(baseList, bucketName, password);
    }

    @Override
    public NodeLocator createLocator(List<MemcachedNode> nodes) {
      return new ArrayModNodeLocator(nodes, getHashAlg());
    }
  }

}
