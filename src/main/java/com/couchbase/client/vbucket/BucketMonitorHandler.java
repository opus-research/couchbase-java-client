package com.couchbase.client.vbucket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.CharsetUtil;
import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

public class BucketMonitorHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(BucketMonitorHandler.class);
  private static final String END_OF_CHUNK = "\n\n\n\n";

  private BucketMonitor bucketMonitor;
  private StringBuilder configChunks;
  private String currentConfig;

  public BucketMonitorHandler() {
    configChunks = new StringBuilder();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
    throws Exception {

    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;
      if (response.getStatus().code() != 200) {
        LOGGER.warn("Streaming connection did not return a 200 response.");
        if (bucketMonitor != null) {
          bucketMonitor.notifyDisconnected();
          return;
        }
      }
    } else if (msg instanceof HttpContent) {
      HttpContent content = (HttpContent) msg;
      String chunk = content.content().toString(CharsetUtil.UTF_8);
      configChunks.append(chunk);
      checkAndNotify();
    }

    super.channelRead(ctx, msg);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    LOGGER.debug("Streaming connection channel active.");
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx)
    throws Exception {
    LOGGER.debug("Streaming connection channel inactive.");
    if (bucketMonitor != null) {
      bucketMonitor.notifyDisconnected();
    }
    super.channelInactive(ctx);
  }

  public void setBucketMonitor(final BucketMonitor bucketMonitor) {
    this.bucketMonitor = bucketMonitor;
  }

  public String getCurrentConfig() {
    return currentConfig;
  }

  private void checkAndNotify() {
    if (configChunks.lastIndexOf(END_OF_CHUNK) > 0) {
      currentConfig = configChunks.toString().trim();
      configChunks.setLength(0);
      bucketMonitor.replaceConfig();
    }
  }

}
