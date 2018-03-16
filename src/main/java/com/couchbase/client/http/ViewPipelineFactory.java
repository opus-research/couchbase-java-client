package com.couchbase.client.http;

import com.couchbase.client.protocol.views.HttpOperation;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Represents the Netty Channel Pipeline for Views.
 */
public class ViewPipelineFactory implements ChannelPipelineFactory {

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