package com.couchbase.client.java.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.util.Map;

public class JacksonJsonConverter implements Converter {

  private final ObjectMapper mapper;

  public JacksonJsonConverter() {
    mapper = new ObjectMapper();
  }

  @Override
  public Object from(ByteBuf buffer) {
    try {
      return mapper.readValue(buffer.toString(CharsetUtil.UTF_8), Map.class);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ByteBuf to(Object content) {
    try {
      return Unpooled.copiedBuffer(mapper.writeValueAsString(content), CharsetUtil.UTF_8);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
