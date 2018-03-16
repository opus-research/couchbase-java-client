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

import com.couchbase.client.protocol.views.HttpOperation;
import com.couchbase.client.vbucket.config.Bucket;
import net.spy.memcached.compat.SpyObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.util.CharsetUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Implements the {@link ViewConnection} functionality on top of netty.
 */
public class NettyViewConnection extends SpyObject implements ViewConnection {

  /**
   * Default connect timeout in seconds.
   */
  public static final int DEFAULT_CONNECT_TIMEOUT = 10;

  /**
   * Default disconnect timeout in seconds.
   */
  public static final int DEFAULT_SHUTDOWN_TIMEOUT = 60;

  /**
   * Contains the bootstrap for all nodes that need to be connected.
   */
  private final ClientBootstrap nodeBoostrap;

  /**
   * A thread-safe list of all connected channels.
   */
  private final List<Channel> connectedChannels;

  /**
   * Indicates if currently connecting.
   */
  private AtomicBoolean connecting;

  /**
   * Indicates running/shut down state.
   */
  private AtomicBoolean running;

  /**
   * Indicates the next node for addOp selection.
   */
  private volatile int nextNode;

  public NettyViewConnection(List<InetSocketAddress> addrs) {
    this.connecting = new AtomicBoolean(false);
    this.running = new AtomicBoolean(false);

    this.connectedChannels = Collections.synchronizedList(
      new ArrayList<Channel>()
    );

    this.nodeBoostrap = new ClientBootstrap(
      new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()
      )
    );
    this.nodeBoostrap.setPipelineFactory(new ViewPipelineFactory());
    bootstrapNodes(addrs);
    this.running.set(true);
  }

  /**
   * Connects and bootstraps the given list of nodes.
   *
   * @param addrs the addrs to connect to.
   */
  private void bootstrapNodes(final List<InetSocketAddress> addrs) {
    if (!connecting.compareAndSet(false, true)) {
      getLogger().debug("Surpressing duplicate bootstrap attempt.");
      return;
    }

    final CountDownLatch connectLatch = new CountDownLatch(addrs.size());
    for (InetSocketAddress addr : addrs) {
      ChannelFuture future = nodeBoostrap.connect(addr);
      future.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            connectedChannels.add(future.getChannel());
          } else {
            getLogger().info("Could not connect to a View Node upon bootstrap.",
              future.getCause());
          }
          connectLatch.countDown();
        }
      });
    }

    try {
      connectLatch.await(DEFAULT_CONNECT_TIMEOUT, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Got interrupted while waiting on View Nodes "
        + "to connect.");
    }

    /**
     * TODO: fix this and make the nodes who are not connected put into a
     * reconnect queue.
     */
    if (connectedChannels.size() < addrs.size()) {
      throw new RuntimeException("Could not connect to all nodes on bootstrap.");
    }
  }

  @Override
  public void addOp(HttpOperation op) {
    if (connectedChannels.isEmpty()) {
      getLogger().error("No server connections. Cancelling op.");
      op.cancel();
    }

    Channel selected = null;
    while (selected == null) {
      int next = getNextChannelIndex();
      if (connectedChannels.get(next).isWritable()) {
        selected = connectedChannels.get(next);
      } else {
        // not writable? TODO: put into reconnect queue
        // this reconnect needs to withstand duplicate
        // additions..
      }
    }

    selected.write(op).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          getLogger().warn("Could not write to View Node: " + future.getCause());
        }
      }
    });

  }

  /**
   * Iterates the connected channels and returns the next node.
   *
   * @return the next node in the list.
   */
  private int getNextChannelIndex() {
    return nextNode = (++nextNode % connectedChannels.size());
  }

  @Override
  public boolean shutdown() throws IOException {
    if (!running.compareAndSet(true, false)) {
      getLogger().debug("Surpressing duplicate shutdown attempt.");
      return true;
    }

    boolean success = true;
    final CountDownLatch disconnectLatch = new CountDownLatch(
      connectedChannels.size());

    synchronized (connectedChannels) {
      Iterator<Channel> iter = connectedChannels.iterator();
      while(iter.hasNext()) {
        Channel channel = iter.next();
        channel.disconnect().addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
              getLogger().info("Could not properly shut down View Node because"
                + "of: ", future.getCause());
            }
            disconnectLatch.countDown();
          }
        });
      }
    }

    try {
      disconnectLatch.await(DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Got interrupted while waiting on View Nodes "
        + "to shutdown.");
    }

    nodeBoostrap.releaseExternalResources();
    return success;
  }

  @Override
  public void checkState() {
    if (!running.get()) {
      throw new IllegalStateException("Shutting down");
    }
  }

  @Override
  public void reconfigure(Bucket bucket) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * TODO: Remove this!
   *
   * @return
   */
  @Override
  public List<DefaultViewNode> getConnectedNodes() {
    throw new UnsupportedOperationException("remove me, I dont belong here.");
  }

  /**
   * Returns all currently connected channels.
   *
   * This method is used for testing purposes.
   *
   * @return the list of connected {@link Channel} instances.
   */
  List<Channel> getConnectedChannels() {
    return connectedChannels;
  }

  /**
   * Represents the Netty Channel Pipeline for Views.
   */
  static class ViewPipelineFactory implements ChannelPipelineFactory {

    private final Queue<HttpOperation> opQueue;

    public ViewPipelineFactory() {
      opQueue = new ConcurrentLinkedQueue<HttpOperation>();
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = pipeline();
      pipeline.addLast("codec", new HttpClientCodec());
      pipeline.addLast("aggregator", new HttpChunkAggregator(Integer.MAX_VALUE));
      pipeline.addLast("responseHandler", new ViewClientResponseHandler(opQueue));
      pipeline.addLast("requestHandler", new ViewClientRequestHandler(opQueue));
      return pipeline;
    }
  }

  /**
   * Parses responses and answers futures.
   */
  static class ViewClientResponseHandler extends SimpleChannelUpstreamHandler {

    private final Queue<HttpOperation> opQueue;
    private final ProtocolVersion protocolVersion =
      new ProtocolVersion("HTTP", 1, 1);

    public ViewClientResponseHandler(Queue<HttpOperation> opQueue) {
      this.opQueue = opQueue;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event)
      throws Exception {
      if (event.getMessage() instanceof DefaultHttpResponse) {
        HttpOperation op = opQueue.poll();
        DefaultHttpResponse httpResponse = (DefaultHttpResponse) event.getMessage();

        HttpResponse response = new BasicHttpResponse(
          protocolVersion,
          httpResponse.getStatus().getCode(),
          httpResponse.getStatus().getReasonPhrase()
        );

        response.setEntity(new StringEntity(
          httpResponse.getContent().toString(CharsetUtil.UTF_8)
        ));

        op.handleResponse(response);
      } else {
        throw new IllegalArgumentException("Got event type I do not "
          + "understand: " + event.getClass().getCanonicalName());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
      if (e instanceof UnresolvedAddressException) {
        throw new RuntimeException("Could not connect to node: "
          + ctx.getChannel().getRemoteAddress() + "because of: ", e.getCause());
      } else {
        super.exceptionCaught(ctx, e);
      }
    }

  }

  /**
   * Handler that converts view requests into netty http requests.
   */
  static class ViewClientRequestHandler extends OneToOneEncoder {

    private final Queue<HttpOperation> opQueue;

    public ViewClientRequestHandler(Queue<HttpOperation> opQueue) {
      this.opQueue = opQueue;
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (msg instanceof HttpOperation) {
        HttpOperation op = (HttpOperation) msg;
        BasicHttpRequest request = (BasicHttpRequest) op.getRequest();

        opQueue.add(op);
        DefaultHttpRequest req = new DefaultHttpRequest(
          HttpVersion.HTTP_1_1,
          HttpMethod.valueOf(request.getRequestLine().getMethod()),
          request.getRequestLine().getUri()
        );
        req.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

        if (request instanceof BasicHttpEntityEnclosingRequest) {
          HttpEntity entity =
            ((BasicHttpEntityEnclosingRequest) request).getEntity();
          if (entity != null) {
            String payload = EntityUtils.toString(entity);
            req.setContent(ChannelBuffers.copiedBuffer(
              payload,
              CharsetUtil.UTF_8)
            );
            req.setHeader(HttpHeaders.CONTENT_LENGTH, payload.length());
          }
        }

        return req;
      } else {
        throw new IllegalArgumentException("Got message type I do not "
          + "understand: " + msg.getClass().getCanonicalName());
      }
    }
  }

}
