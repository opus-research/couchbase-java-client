package com.couchbase.client.java;

import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.InsertRequest;
import com.couchbase.client.core.message.binary.InsertResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import com.couchbase.client.java.convert.Converter;
import com.couchbase.client.java.convert.JacksonJsonConverter;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import io.netty.buffer.ByteBuf;
import rx.Observable;
import rx.functions.Func1;

import java.util.HashMap;
import java.util.Map;

public class CouchbaseBucket implements Bucket {

  private final String bucket;
  private final Cluster core;

  private final Map<Class<?>, Converter> converters;

  public CouchbaseBucket(final Cluster core, final String name) {
    bucket = name;
    this.core = core;

    converters = new HashMap<Class<?>, Converter>();
    converters.put(JsonDocument.class, new JacksonJsonConverter());
  }

  @Override
  public Observable<JsonDocument> get(final String id) {
    return get(id, JsonDocument.class);
  }

  @Override
  public <D extends Document> Observable<D> get(final String id, final Class<D> target) {
    return core.<GetResponse>send(new GetRequest(id, bucket))
      .map(new Func1<GetResponse, D>() {
        @Override
        public D call(final GetResponse response) {
          Converter converter = converters.get(target);
          Object content = response.status() == ResponseStatus.OK ? converter.from(response.content()) : null;
          Document document = null;
          if (target.isAssignableFrom(JsonDocument.class)) {
            document = new JsonDocument(id, content, response.cas(), 0);
          }
          return (D) document;
        }
      }
    );
  }

  @Override
  public <D extends Document> Observable<D> insert(final D document) {
    Converter converter = converters.get(document.getClass());
    ByteBuf content = converter.to(document.content());
    return core
      .<InsertResponse>send(new InsertRequest(document.id(), content, bucket))
      .map(new Func1<InsertResponse, D>() {
        @Override
        public D call(InsertResponse response) {
          return document;
        }
      });
  }

  @Override
  public <D extends Document> Observable<D> upsert(D document) {
    Converter converter = converters.get(document.getClass());
    ByteBuf content = converter.to(document.content());
    return core
      .<UpsertResponse>send(new UpsertRequest(document.id(), content, bucket))
      .map(new Func1<UpsertResponse, D>() {
        @Override
        public D call(UpsertResponse response) {
          return document;
        }
      });
  }
}
