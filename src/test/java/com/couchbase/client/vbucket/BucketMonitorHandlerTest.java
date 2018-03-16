package com.couchbase.client.vbucket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.junit.Before;
import org.junit.Test;

public class BucketMonitorHandlerTest {

  private EmbeddedChannel channel;

  @Before
  public void setup() {
    channel = new EmbeddedChannel(new BucketMonitorHandler());
  }

  @Test
  public void shouldDecodeChunkedConfig() {
    ByteBuf config = Unpooled.copiedBuffer("{\"foo\": 1}\n\n\n\n", CharsetUtil.UTF_8);

    channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK));
    channel.writeInbound(new DefaultLastHttpContent(config));
  }

}
