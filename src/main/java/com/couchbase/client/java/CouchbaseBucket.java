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
package com.couchbase.client.java;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.InsertRequest;
import com.couchbase.client.core.message.binary.InsertResponse;
import com.couchbase.client.core.message.binary.RemoveRequest;
import com.couchbase.client.core.message.binary.RemoveResponse;
import com.couchbase.client.core.message.binary.ReplaceRequest;
import com.couchbase.client.core.message.binary.ReplaceResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import com.couchbase.client.core.message.config.FlushRequest;
import com.couchbase.client.core.message.config.FlushResponse;
import com.couchbase.client.core.message.document.CoreDocument;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.java.bucket.ViewQueryMapper;
import com.couchbase.client.java.convert.BinaryConverter;
import com.couchbase.client.java.convert.Converter;
import com.couchbase.client.java.convert.JacksonJsonConverter;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ViewQuery;
import com.couchbase.client.java.query.ViewResult;
import rx.Observable;
import rx.functions.Func1;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Michael Nitschinger
 * @author David Sondermann
 * @since 2.0
 */
public class CouchbaseBucket implements Bucket {

  private final String bucket;
  private final String password;
  private final ClusterFacade core;
  private final Map<Class<?>, Converter> converters;

  public CouchbaseBucket(final ClusterFacade core, final String name, final String password) {
    this.bucket = name;
    this.password = password;
    this.core = core;

    converters = new HashMap<Class<?>, Converter>();
    converters.put(JsonDocument.class, new JacksonJsonConverter());
    converters.put(BinaryDocument.class, new BinaryConverter());
  }

  @Override
  public Observable<JsonDocument> get(final String id) {
    return get(id, JsonDocument.class);
  }

  @Override
  public <D extends Document<?>> Observable<D> get(final String id, final Class<D> target) {
    return core
        .<GetResponse>send(new GetRequest(id, bucket))
        .map(new Func1<GetResponse, D>() {
               @Override
               public D call(final GetResponse response) {
                 final Converter converter = converters.get(target);
                 return converter.decode(response.document());
               }
             }
        );
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D extends Document<?>> Observable<D> insert(final D document) {
    final Converter converter = converters.get(document.getClass());
    final CoreDocument coreDocument = converter.encode(document);

    return core
        .<InsertResponse>send(new InsertRequest(coreDocument, bucket))
        .map(new Func1<InsertResponse, D>() {
          @Override
          public D call(final InsertResponse response) {
            return (D) document.copy(response.cas(), response.status());
          }
        });
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D extends Document<?>> Observable<D> upsert(final D document) {
    final Converter converter = converters.get(document.getClass());
    final CoreDocument coreDocument = converter.encode(document);

    return core
        .<UpsertResponse>send(new UpsertRequest(coreDocument, bucket))
        .map(new Func1<UpsertResponse, D>() {
          @Override
          public D call(final UpsertResponse response) {
            return (D) document.copy(response.cas(), response.status());
          }
        });
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D extends Document<?>> Observable<D> replace(final D document) {
    final Converter converter = converters.get(document.getClass());
    final CoreDocument coreDocument = converter.encode(document);

    return core
        .<ReplaceResponse>send(new ReplaceRequest(coreDocument, bucket))
        .map(new Func1<ReplaceResponse, D>() {
          @Override
          public D call(final ReplaceResponse response) {
            return (D) document.copy(response.cas(), response.status());
          }
        });
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D extends Document<?>> Observable<D> remove(final D document) {
    return core
        .<RemoveResponse>send(new RemoveRequest(document.id(), document.cas(), bucket))
        .map(new Func1<RemoveResponse, D>() {
          @Override
          public D call(final RemoveResponse response) {
            return (D) document.copy(document.cas(), response.status());
          }
        });
  }

  @Override
  public Observable<JsonDocument> remove(final String id) {
    return remove(JsonDocument.create(id));
  }

  @Override
  public Observable<ViewResult> query(final ViewQuery query) {
    final ViewQueryRequest request = new ViewQueryRequest(query.getDesign(), query.getView(), query.isDevelopment(), query.toString(), bucket, password);

    return core
        .<ViewQueryResponse>send(request)
        .flatMap(new ViewQueryMapper(converters))
        .map(new Func1<JsonObject, ViewResult>() {
               @Override
               public ViewResult call(final JsonObject object) {
                 return new ViewResult(object.getString("id"), object.getString("key"), object.get("value"));
               }
             }
        );
  }

  @Override
  public Observable<QueryResult> query(final Query query) {
    return query(query.toString());
  }

  @Override
  public Observable<QueryResult> query(final String query) {
    final Converter converter = converters.get(JsonDocument.class);
    final GenericQueryRequest request = new GenericQueryRequest(query, bucket, password);

    return core
        .<GenericQueryResponse>send(request)
        .filter(new Func1<GenericQueryResponse, Boolean>() {
          @Override
          public Boolean call(final GenericQueryResponse response) {
            return response.content() != null;
          }
        })
        .map(new Func1<GenericQueryResponse, QueryResult>() {
          @Override
          public QueryResult call(final GenericQueryResponse response) {
            return new QueryResult(((JacksonJsonConverter) converter).decode(response.content()));
          }
        });
  }

  @Override
  public Observable<FlushResponse> flush()
  {
    final FlushRequest request = new FlushRequest(bucket, password);

    return core.send(request);
  }
}
