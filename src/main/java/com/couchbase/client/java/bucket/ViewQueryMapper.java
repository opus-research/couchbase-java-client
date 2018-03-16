/**
 * Copyright (C) 2014 Couchbase, Inc.
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
package com.couchbase.client.java.bucket;

import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.java.convert.Converter;
import com.couchbase.client.java.convert.JacksonJsonConverter;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.util.CharsetUtil;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ViewQueryMapper implements Func1<ViewQueryResponse, Observable<JsonObject>> {

  private final Map<Class<?>, Converter> converters;

  public ViewQueryMapper(Map<Class<?>, Converter> converters) {
    this.converters = converters;
  }

  @Override
  public Observable<JsonObject> call(final ViewQueryResponse response) {
    final Converter converter = converters.get(JsonDocument.class);
    final MarkersProcessor processor = new MarkersProcessor();
    response.content().forEachByte(processor);

    final List<Integer> markers = processor.markers();
    final List<JsonObject> objects = new ArrayList<JsonObject>();
    for (final Integer marker : markers) {
      final ByteBuf chunk = response.content().readBytes(marker - response.content().readerIndex());
      chunk.readerIndex(chunk.readerIndex() + 1);
      objects.add(((JacksonJsonConverter) converter).decode(chunk.toString(CharsetUtil.UTF_8)));
    }
    return Observable.from(objects);
  }

  static class MarkersProcessor implements ByteBufProcessor {
    private static final byte open = '{';
    private static final byte close = '}';

    private final List<Integer> markers = new ArrayList<Integer>();

    private int depth;
    private int counter;

    @Override
    public boolean process(byte value) throws Exception {
      counter++;
      if (value == open) {
        depth++;
      }
      if (value == close) {
        depth--;
        if (depth == 0) {
          markers.add(counter);
        }
      }

      return true;
    }

    public List<Integer> markers() {
      return markers;
    }
  }
}
