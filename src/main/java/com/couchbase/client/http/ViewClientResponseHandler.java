package com.couchbase.client.http;

import com.couchbase.client.protocol.views.HttpOperation;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.util.CharsetUtil;

import java.nio.channels.UnresolvedAddressException;
import java.util.Queue;

/**
 * Parses responses and answers futures.
 */
class ViewClientResponseHandler extends SimpleChannelUpstreamHandler {

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