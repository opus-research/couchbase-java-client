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
import com.couchbase.client.vbucket.config.ConfigurationParser;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.ParseException;
import java.util.Observable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

/**
 * The BucketMonitor will open an HTTP comet stream to monitor for changes to
 * the list of nodes. If the list of nodes changes, it will notify observers.
 */
public class BucketMonitor extends Observable {

  private final URI cometStreamURI;
  private final String httpUser;
  private final String httpPass;
  private volatile Channel channel;
  private final String host;
  private final int port;
  private ConfigurationParser configParser;
  private BucketMonitorHandler handler;
  private final HttpHeaders headers;
  private static final Logger LOGGER =
    LoggerFactory.getLogger(BucketMonitor.class.getName());
  private Bootstrap bootstrap;
  private final ConfigurationProviderHTTP provider;
  private final EventLoopGroup group;

  /**
   * @param cometStreamURI the URI which will stream node changes
   * @param username the username required for HTTP Basic Auth to the restful
   *          service
   * @param password the password required for HTTP Basic Auth to the restful
   *          service
   */
  public BucketMonitor(URI cometStreamURI, String username, String password,
    ConfigurationParser configParser, ConfigurationProviderHTTP provider) {
    if (cometStreamURI == null) {
      throw new IllegalArgumentException("cometStreamURI cannot be NULL");
    }
    String scheme = cometStreamURI.getScheme() == null ? "http"
        : cometStreamURI.getScheme();
    if (!scheme.equals("http")) {
      // an SslHandler is needed in the pipeline
      throw new UnsupportedOperationException("Only http is supported.");
    }

    this.cometStreamURI = cometStreamURI;
    httpUser = username;
    httpPass = password;
    this.configParser = configParser;
    host = cometStreamURI.getHost();
    port = cometStreamURI.getPort() == -1 ? 80 : cometStreamURI.getPort();
    headers = new DefaultHttpHeaders();
    this.provider = provider;
    group = new NioEventLoopGroup();
  }

  /**
   * Take any action required when the monitor appears to be disconnected.
   */
  protected void notifyDisconnected() {
    Bucket bucket = provider.getBucketConfiguration(provider.getBucket());
    bucket.setIsNotUpdating();
    LOGGER.trace("Marked bucket " + bucket.getName()
      + " as not updating.  Notifying observers.");
    LOGGER.trace("There appear to be " + this.countObservers()
      + " observers waiting for notification");
    setChanged();
    notifyObservers(bucket);
  }

  public void startMonitor() {
    if (channel != null) {
      LOGGER.info("Bucket monitor is already started.");
      return;
    }

    ChannelFuture channelFuture = createChannel();

    final CountDownLatch channelLatch = new CountDownLatch(1);
    channelFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture cf) throws Exception {
        if(cf.isSuccess()) {
          channel = cf.channel();
        } else {
          LOGGER.warn("Could not start monitor channel because of: ",
            cf.cause());
        }
        channelLatch.countDown();
      }
    });

    try {
      channelLatch.await();
    } catch(InterruptedException ex) {
      throw new ConnectionException("Interrupted while waiting for streaming "
        + "connection to arrive.");
    }

    if (channel == null) {
      shutdown();
      throw new ConnectionException("Could not establish a streaming connection to "
        + host + ":" + port);
    }

    handler = (BucketMonitorHandler) channel.pipeline().get("handler");
    handler.setBucketMonitor(this);
    HttpRequest request = prepareRequest(cometStreamURI, host);
    channel.writeAndFlush(request);

    try {
      String response = handler.getCurrentConfig();
      LOGGER.debug("Getting server list returns this last chunked response:\n"
          + response);
      Bucket bucketToMonitor = configParser.parseBucket(response);
      setChanged();
      notifyObservers(bucketToMonitor);
    } catch (ParseException ex) {
      LOGGER.warn("Invalid client configuration received from server. "
        + "Staying with existing configuration.", ex);
      LOGGER.debug("Invalid client configuration received:\n",
        handler.getCurrentConfig());
    }
  }

  protected ChannelFuture createChannel() {
    bootstrap = new Bootstrap();
    bootstrap
      .group(new NioEventLoopGroup())
      .channel(NioSocketChannel.class)
      .handler(new BucketMonitorChannelInitializer());

    return bootstrap.connect(new InetSocketAddress(host, port));
  }

  protected HttpRequest prepareRequest(URI uri, String h) {
    // Send the HTTP request.
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
      HttpMethod.GET, uri.toASCIIString());
    headers.setHeader(request, HttpHeaders.Names.HOST, h);
    if (getHttpUser() != null && !getHttpUser().isEmpty()) {
      String basicAuthHeader;
      try {
        basicAuthHeader =
          HttpUtil.buildAuthHeader(getHttpUser(),
            getHttpPass());
        headers.setHeader(request, HttpHeaders.Names.AUTHORIZATION,
          basicAuthHeader);
      } catch (UnsupportedEncodingException ex) {
        throw new RuntimeException("Could not encode specified credentials"
            + " for HTTP request.", ex);
      }
    }
    headers.setHeader(request, HttpHeaders.Names.CONNECTION,
      HttpHeaders.Values.CLOSE);  // No keep-alives for this
    headers.setHeader(request, HttpHeaders.Names.CACHE_CONTROL,
      HttpHeaders.Values.NO_CACHE);
    headers.setHeader(request, HttpHeaders.Names.ACCEPT, "application/json");
    headers.setHeader(request, HttpHeaders.Names.USER_AGENT,
      "Couchbase Java Client");
    return request;
  }

  /**
   * @return the httpUser
   */
  public String getHttpUser() {
    return httpUser;
  }

  /**
   * @return the httpPass
   */
  public String getHttpPass() {
    return httpPass;
  }

  /**
   * Shut down the monitor in a graceful way (and release all resources).
   */
  public void shutdown() {
    shutdown(-1, TimeUnit.MILLISECONDS);
  }

  /**
   * Shut down this monitor in a graceful way.
   *
   * @param timeout
   * @param unit
   */
  public void shutdown(long timeout, TimeUnit unit) {
    deleteObservers();
    if (timeout < 0) {
      group.shutdownGracefully();
    } else {
      group.shutdownGracefully(0, timeout, unit);
    }
  }

  /**
   * Replace the previously received configuration with the current one.
   */
  protected void replaceConfig() {
    try {
      String response = handler.getCurrentConfig();
      Bucket updatedBucket = this.configParser.updateBucket(
        response,
        provider.getBucketConfiguration(provider.getBucket())
      );
      setChanged();
      notifyObservers(updatedBucket);
    } catch (ParseException e) {
      LOGGER.warn("Invalid client configuration received from server. Staying with "
        +  "existing configuration.", e);
    }
  }

  public void setConfigParser(ConfigurationParser newConfigParser) {
    this.configParser = newConfigParser;
  }
}
