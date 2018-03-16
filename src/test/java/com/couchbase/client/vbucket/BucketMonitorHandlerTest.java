package com.couchbase.client.vbucket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BucketMonitorHandlerTest {

  private EmbeddedChannel channel;
  private BucketMonitor monitorMock;
  private BucketMonitorHandler handler;

  @Before
  public void setup() {
    monitorMock = mock(BucketMonitor.class);
    handler = new BucketMonitorHandler();
    channel = new EmbeddedChannel(handler);
    handler.setBucketMonitor(monitorMock);
  }

  @Test
  public void shouldDecodeChunkedConfig() {
    String config1 = "{\"foo\": 1}\n\n\n\n";
    String config2 = "{\"foo\": 2}\n\n\n\n";

    channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK));

    channel.writeInbound(new DefaultHttpContent(
      Unpooled.copiedBuffer(config1, CharsetUtil.UTF_8)));
    assertEquals(config1.trim(), handler.getCurrentConfig());

    channel.writeInbound(new DefaultHttpContent(
      Unpooled.copiedBuffer(config2, CharsetUtil.UTF_8)));
    assertEquals(config2.trim(), handler.getCurrentConfig());

    verify(monitorMock, times(2)).replaceConfig();
  }

  @Test
  public void shouldDecodeSmallChunks() {
    String config = "{\"foo\": 1}\n\n\n\n";

    channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK));

    for (byte configByte : config.getBytes(CharsetUtil.UTF_8)) {
      channel.writeInbound(
        new DefaultHttpContent(Unpooled.buffer().writeByte(configByte))
      );
    }

    assertEquals(config.trim(), handler.getCurrentConfig());
    verify(monitorMock, times(1)).replaceConfig();
  }

}
