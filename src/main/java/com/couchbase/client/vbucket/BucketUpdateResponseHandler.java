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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.CharsetUtil;
import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

/**
 * A BucketUpdateResponseHandler.
 */
public class BucketUpdateResponseHandler extends ChannelInboundHandlerAdapter {

  private volatile boolean readingChunks;
  private String lastResponse;
  private ChannelFuture receivedFuture;
  private CountDownLatch latch;
  private StringBuilder partialResponse;
  private BucketMonitor monitor;

  private static final Logger LOGGER =
    LoggerFactory.getLogger(BucketUpdateResponseHandler.class);


  @Override
  public void channelRead(final ChannelHandlerContext context, final Object event) {
    /*ChannelFuture channelFuture = event.getFuture();
    setReceivedFuture(channelFuture);

    if (this.partialResponse == null) {
      this.partialResponse = new StringBuilder();
    }

    if (readingChunks) {
      HttpChunk chunk = (HttpChunk) event.getMessage();
      if (chunk.isLast()) {
        readingChunks = false;
      } else {
        String curChunk = chunk.getContent().toString(CharsetUtil.UTF_8);
        if (curChunk.matches("\n\n\n\n")) {
          setLastResponse(partialResponse.toString());
          partialResponse = null;
          getLatch().countDown();
          if (monitor != null) {
            monitor.replaceConfig();
          }
        } else {
          finerLog(curChunk);
          finerLog("Chunk length is: " + curChunk.length());
          partialResponse.append(curChunk);
          channelFuture.setSuccess();
        }

      }
    } else {
      HttpResponse response = (HttpResponse) event.getMessage();
      logResponse(response);
    }*/
  }

  private void logResponse(HttpResponse response) {
    LOGGER.debug("Streaming Connection - Status: " + response.getStatus()
      + ", Version: " + response.getProtocolVersion());


    if (!response.headers().isEmpty()) {
      for (Map.Entry<String, String> header : response.headers()) {
        LOGGER.debug("HEADER: " + header.getKey() + " = " + header.getValue());
      }
    }

    /*
    if (response.getStatus().code() == 200 && response.isChunked()) {
      readingChunks = true;
      LOGGER.debug("CHUNKED CONTENT {");
    } else if(response.getStatus().code() == 200) {
      ChannelBuffer content = response.getContent();
      if (content.readable()) {
        finerLog("CONTENT {");
        finerLog(content.toString(CharsetUtil.UTF_8));
        finerLog("} END OF CONTENT");
      }
    } else {
      throw new ConnectionException("Could not retrieve configuration chunk. "
        + "Response Code is: " + response.getStatus());
    }
    */
  }

  /**
   * @return the lastResponse
   */
  protected String getLastResponse() {
    ChannelFuture channelFuture = getReceivedFuture();

    if (channelFuture.awaitUninterruptibly(30, TimeUnit.SECONDS)) {
      return lastResponse;
    } else {
      throw new ConnectionException("Cannot contact any server in the pool");
    }
  }

  /**
   * @param newLastResponse the lastResponse to set
   */
  private void setLastResponse(String newLastResponse) {
    lastResponse = newLastResponse;
  }

  /**
   * @return the receivedFuture
   */
  private ChannelFuture getReceivedFuture() {
    try {
      getLatch().await();
    } catch (InterruptedException ex) {
      LOGGER.debug("Receiving Channel streaming future has been interrupted.");
    }
    return receivedFuture;
  }

  /**
   * @param newReceivedFuture the receivedFuture to set
   */
  private void setReceivedFuture(ChannelFuture newReceivedFuture) {
    receivedFuture = newReceivedFuture;
  }

  /**
   * @return the latch
   */
  private CountDownLatch getLatch() {
    if (latch == null) {
      latch = new CountDownLatch(1);
    }
    return latch;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    LOGGER.debug("Streaming connection channel active.");
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOGGER.debug("Streaming connection channel inactive.");
    if (monitor != null) {
      monitor.notifyDisconnected();
    }
    super.channelInactive(ctx);
  }

  protected void setBucketMonitor(BucketMonitor newMonitor) {
    monitor = newMonitor;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e)
    throws Exception {
    LOGGER.warn("Streaming connection exception caught: ", e);

    StringBuilder sb = new StringBuilder();
    for (StackTraceElement one : e.getStackTrace()) {
      sb.append(one.toString());
      sb.append("\n");
    }
    LOGGER.warn(sb.toString());

    if (monitor != null) {
      monitor.replaceConfig();
    }
  }


}
