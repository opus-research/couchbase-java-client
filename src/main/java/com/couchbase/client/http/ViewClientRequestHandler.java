package com.couchbase.client.http;

import com.couchbase.client.protocol.views.HttpOperation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.util.CharsetUtil;

import java.util.Queue;

/**
 * Handler that converts view requests into netty http requests.
 */
class ViewClientRequestHandler extends OneToOneEncoder {

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
          req.setContent(ChannelBuffers.copiedBuffer(payload, CharsetUtil.UTF_8)
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