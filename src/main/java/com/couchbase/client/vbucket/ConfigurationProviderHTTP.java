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

package com.couchbase.client.vbucket;

import com.couchbase.client.http.HttpUtil;
import com.couchbase.client.vbucket.config.Bucket;
import com.couchbase.client.vbucket.config.Config;
import com.couchbase.client.vbucket.config.ConfigType;
import com.couchbase.client.vbucket.config.ConfigurationParser;
import com.couchbase.client.vbucket.config.ConfigurationParserJSON;
import com.couchbase.client.vbucket.config.Pool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

import java.text.ParseException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.compat.SpyObject;

/**
 * A configuration provider.
 */
public class ConfigurationProviderHTTP extends SpyObject implements
    ConfigurationProvider {
  /**
   * Configuration management class that provides methods for retrieving vbucket
   * configuration and receiving configuration updates.
   */
  private static final String DEFAULT_POOL_NAME = "default";
  private static final String ANONYMOUS_AUTH_BUCKET = "default";
  /**
   * The specification version which this client meets. This will be included in
   * requests to the server.
   */
  public static final String CLIENT_SPEC_VER = "1.0";
  private volatile List<URI> baseList;
  private String restUsr;
  private String restPwd;
  private URI loadedBaseUri;

  private final Map<String, Bucket> buckets =
    new ConcurrentHashMap<String, Bucket>();

  private ConfigurationParser configurationParser =
    new ConfigurationParserJSON();
  private Map<String, BucketMonitor> monitors =
    new HashMap<String, BucketMonitor>();
  private volatile String reSubBucket;
  private volatile Reconfigurable reSubRec;

  /**
   * Constructs a configuration provider with disabled authentication for the
   * REST service.
   *
   * @param baseList list of urls to treat as base
   * @throws IOException
   */
  public ConfigurationProviderHTTP(List<URI> baseList) throws IOException {
    this(baseList, null, null);
  }

  /**
   * Constructs a configuration provider with a given credentials for the REST
   * service.
   *
   * @param baseList list of urls to treat as base
   * @param restUsr username
   * @param restPwd password
   */
  public ConfigurationProviderHTTP(List<URI> baseList, String restUsr,
      String restPwd) {
    this.baseList = baseList;
    this.restUsr = restUsr;
    this.restPwd = restPwd;
  }

  /**
   * Returns the current Reconfigurable object.
   */
  @Override
  public synchronized Reconfigurable getReconfigurable() {
    return reSubRec;
  }

  /**
   * Returns the current bucket name.
   */
  @Override
  public synchronized String getBucket() {
    return reSubBucket;
  }


  /**
   * Connects to the REST service and retrieves the bucket configuration from
   * the first pool available.
   *
   * @param bucketname bucketname
   * @return vbucket configuration
   */
  public Bucket getBucketConfiguration(final String bucketname) {
    if (bucketname == null || bucketname.isEmpty()) {
      throw new IllegalArgumentException("Bucket name can not be blank.");
    }
    Bucket bucket = this.buckets.get(bucketname);
    if (bucket == null) {
      boolean warmedUp = false;
      int maxBackoffRetries = 5;
      int retryCount = 1;
      while(!warmedUp) {
        readPools(bucketname);
        Config config = this.buckets.get(bucketname).getConfig();
        if(config.getConfigType().equals(ConfigType.MEMCACHE)) {
          warmedUp = true;
          continue;
        }
        if(config.getVbucketsCount() == 0) {
          if(retryCount > maxBackoffRetries) {
            throw new ConfigurationException("Cluster is not in a warmed up "
              + "state after " + maxBackoffRetries + " exponential retries.");
          }
          int backoffSeconds = new Double(
            retryCount * Math.pow(2, retryCount++)).intValue();
          getLogger().info("Cluster is currently warming up, waiting "
            + backoffSeconds + " seconds for vBuckets to show up.");
          try {
            Thread.sleep(backoffSeconds * 1000);
          } catch (InterruptedException ex) {
            throw new ConfigurationException("Cluster is not in a warmed up "
              + "state.");
          }
        } else {
          warmedUp = true;
        }
      }
    }
    return this.buckets.get(bucketname);
  }

  /**
   * Update the configuration provider with a new bucket.
   *
   * This method is usually called from the CouchbaseClient class during
   * reconfiguration to make sure the configuration provider has the most
   * recent bucket available (including the enclosed config).
   *
   * @param bucketname the name of the bucket.
   * @param newBucket the new bucket to update.
   */
  @Override
  public void updateBucket(String bucketname, Bucket newBucket) {
    this.buckets.put(bucketname, newBucket);
  }

  /**
   * For a given bucket to be found, walk the URIs in the baselist until the
   * bucket needed is found.
   *
   * The intent with this method is to encapsulate all of the walking of
   * URIs and populating an internal object model of the configuration in
   * one place.
   *
   * When the full baseList URIs are walked and still no connection is
   * established, a backoff algorithm is in place to retry after a
   * increasing timeframe.
   *
   * @param bucketToFind
   */
  private void readPools(String bucketToFind) {
    for (URI baseUri : baseList) {
      try {
        // get and parse the response from the current base uri
        URLConnection baseConnection = urlConnBuilder(null, baseUri);
        String base = readToString(baseConnection);
        if ("".equals(base)) {
          getLogger().warn("Provided URI " + baseUri + " has an empty"
            + " response... skipping");
          continue;
        }
        Map<String, Pool> pools = this.configurationParser.parseBase(base);

        // check for the default pool name
        if (!pools.containsKey(DEFAULT_POOL_NAME)) {
          getLogger().warn("Provided URI " + baseUri + " has no default pool"
            + "... skipping");
          continue;
        }


        // load pools
        for (Pool pool : pools.values()) {
          URLConnection poolConnection = urlConnBuilder(baseUri,
            pool.getUri());
          String poolString = readToString(poolConnection);
          configurationParser.loadPool(pool, poolString);
          URLConnection poolBucketsConnection = urlConnBuilder(baseUri,
            pool.getBucketsUri());
          String sBuckets = readToString(poolBucketsConnection);
          Map<String, Bucket> bucketsForPool =
              configurationParser.parseBuckets(sBuckets);
          pool.replaceBuckets(bucketsForPool);
        }

        // did we find our bucket?
        boolean bucketFound = false;
        for (Pool pool : pools.values()) {
          if (pool.hasBucket(bucketToFind)) {
            bucketFound = true;
            break;
          }
        }
        if (bucketFound) {
          for (Pool pool : pools.values()) {
            for (Map.Entry<String, Bucket> bucketEntry : pool.getROBuckets()
                .entrySet()) {
              this.buckets.put(bucketEntry.getKey(), bucketEntry.getValue());
            }
          }

          if (this.buckets.get(bucketToFind) == null) {
            getLogger().warn("Bucket found, but has no bucket "
              + "configuration attached...skipping");
            continue;
          }

          this.loadedBaseUri = baseUri;
          return;
        }
      } catch (ParseException e) {
        getLogger().warn("Provided URI " + baseUri
          + " has an unparsable response...skipping", e);
        continue;
      } catch (IOException e) {
        getLogger().warn("Connection problems with URI " + baseUri
          + " ...skipping", e);
        continue;
      }
    }
    throw new ConfigurationException("Configuration for bucket \""
      + bucketToFind + "\" was not found in server list (" + baseList + ").");
  }

  public List<InetSocketAddress> getServerList(final String bucketname) {
    Bucket bucket = getBucketConfiguration(bucketname);
    List<String> servers = bucket.getConfig().getServers();
    StringBuilder serversString = new StringBuilder();
    for (String server : servers) {
      serversString.append(server).append(' ');
    }
    return AddrUtil.getAddresses(serversString.toString());
  }

  public synchronized void finishResubscribe() {
    monitors.clear();
    subscribe(reSubBucket, reSubRec);
  }

  public synchronized void markForResubscribe(String bucketName,
    Reconfigurable rec) {
    getLogger().debug("Marking bucket " + bucketName
      + " for resubscribe with reconfigurable " + rec);
    reSubBucket = bucketName; // can't subscribe here, must from user request
    reSubRec = rec;
  }

  /**
   * Subscribes for configuration updates.
   *
   * @param bucketName bucket name to receive configuration for
   * @param rec reconfigurable that will receive updates
   */
  public synchronized void subscribe(String bucketName, Reconfigurable rec) {
    if (null == bucketName || (null != reSubBucket
      && !bucketName.equals(reSubBucket))) {
      throw new IllegalArgumentException("Bucket name cannot be null and must"
        + " never be re-set to a new object. Bucket: "
        + bucketName + ", reSubBucket: " + reSubBucket);
    }

    if (null == rec || (null != reSubRec && rec != reSubRec)) {
      throw new IllegalArgumentException("Reconfigurable cannot be null and"
        + " must never be re-set to a new object");
    }
    reSubBucket = bucketName;  // More than one subscriber, would be an error
    reSubRec = rec;

    getLogger().debug("Subscribing an object for reconfiguration updates "
      + rec.getClass().getName());
    Bucket bucket = getBucketConfiguration(bucketName);

    if(bucket == null) {
      throw new ConfigurationException("Could not get bucket configuration "
        + "for: " + bucketName);
    }

    ReconfigurableObserver obs = new ReconfigurableObserver(rec);
    BucketMonitor monitor = this.monitors.get(bucketName);
    if (monitor == null) {
      URI streamingURI = bucket.getStreamingURI();
      monitor = new BucketMonitor(this.loadedBaseUri.resolve(streamingURI),
        bucketName, this.restUsr, this.restPwd, configurationParser);
      this.monitors.put(bucketName, monitor);
      monitor.addObserver(obs);
      monitor.startMonitor();
    } else {
      monitor.addObserver(obs);
    }
  }

  /**
   * Unsubscribe from updates on a given bucket and given reconfigurable.
   *
   * @param vbucketName bucket name
   * @param rec reconfigurable
   */
  public void unsubscribe(String vbucketName, Reconfigurable rec) {
    BucketMonitor monitor = this.monitors.get(vbucketName);
    if (monitor != null) {
      monitor.deleteObserver(new ReconfigurableObserver(rec));
    }
  }

  public Config getLatestConfig(String bucketname) {
    Bucket bucket = getBucketConfiguration(bucketname);
    return bucket.getConfig();
  }

  public String getAnonymousAuthBucket() {
    return ANONYMOUS_AUTH_BUCKET;
  }

  /**
   * Shutdowns a monitor connections to the REST service.
   */
  public void shutdown() {
    for (BucketMonitor monitor : this.monitors.values()) {
      monitor.shutdown();
    }
  }

  /**
   * Create a URL which has the appropriate headers to interact with the
   * service. Most exception handling is up to the caller.
   *
   * @param resource the URI either absolute or relative to the base for this
   * ClientManager
   * @return
   * @throws java.io.IOException
   */
  private URLConnection urlConnBuilder(URI base, URI resource)
    throws IOException {
    if (!resource.isAbsolute() && base != null) {
      resource = base.resolve(resource);
    }
    URL specURL = resource.toURL();
    URLConnection connection = specURL.openConnection();
    connection.setConnectTimeout(500); // All conns are on local LAN
    connection.setRequestProperty("Accept", "application/json");
    connection.setRequestProperty("user-agent", "Couchbase Java Client");
    connection.setRequestProperty("X-memcachekv-Store-Client-"
      + "Specification-Version", CLIENT_SPEC_VER);
    if (restUsr != null) {
      try {
        connection.setRequestProperty("Authorization",
            HttpUtil.buildAuthHeader(restUsr, restPwd));
      } catch (UnsupportedEncodingException ex) {
        throw new IOException("Could not encode specified credentials for "
          + "HTTP request.", ex);
      }
    }
    return connection;
  }

  /**
   * Helper method that reads content from URLConnection to the string.
   *
   * @param connection a given URLConnection
   * @return content string
   * @throws IOException
   */
  private String readToString(URLConnection connection) throws IOException {
    BufferedReader reader = null;
    getLogger().debug("Attempting to read configuration from URI: "
      + connection.getURL());
    try {
      connection.setConnectTimeout(500);
      connection.setReadTimeout(5000);
      InputStream inStream = connection.getInputStream();
      if (connection instanceof java.net.HttpURLConnection) {
        HttpURLConnection httpConnection = (HttpURLConnection) connection;
        if (httpConnection.getResponseCode() == 403) {
          throw new IOException("Service does not accept the authentication "
            + "credentials: " + httpConnection.getResponseCode()
            + httpConnection.getResponseMessage());
        } else if (httpConnection.getResponseCode() >= 400) {
          throw new IOException("Service responded with a failure code: "
            + httpConnection.getResponseCode()
            + httpConnection.getResponseMessage());
        }
      } else {
        throw new IOException("Unexpected URI type encountered");
      }
      reader = new BufferedReader(new InputStreamReader(inStream));
      String str;
      StringBuilder buffer = new StringBuilder();
      while ((str = reader.readLine()) != null) {
        buffer.append(str);
      }
      return buffer.toString();
    } catch (SocketTimeoutException ex) {
      String msg = "Timed out while reading configuration over HTTP";
      getLogger().warn(msg, ex);
      throw new IOException(msg, ex);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Override
  public synchronized String toString() {
    String result = "";
    result += "bucket: " + reSubBucket;
    result += "reconf:" + reSubRec;
    result += "baseList:" + baseList;
    return result;
  }

}
