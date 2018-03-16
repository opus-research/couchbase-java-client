/**
 * Copyright (C) 2015 Couchbase, Inc.
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
package com.couchbase.client.java.repository;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.repository.mapping.EntityProperties;
import com.couchbase.client.java.repository.mapping.ReflectionBasedEntityProperties;
import com.couchbase.client.java.repository.mapping.RepositoryMappingException;
import rx.Observable;
import rx.functions.Func1;

import java.lang.reflect.Field;

public class CouchbaseAsyncRepository implements AsyncRepository {

    private final AsyncBucket bucket;

    public CouchbaseAsyncRepository(AsyncBucket bucket) {
        this.bucket = bucket;
    }

    @Override
    public <T> Observable<T> get(String id, Class<T> documentClass) {
        return null;
    }

    @Override
    public <T> Observable<T> upsert(final T document) {
        EntityProperties properties = new ReflectionBasedEntityProperties(document.getClass());

        if (!properties.hasIdProperty()) {
            return Observable.error(new RepositoryMappingException("No Id Field annotated with @Id present."));
        }

        String id = properties.get(properties.idProperty(), document, String.class);
        if (id == null) {
            return Observable.error(new RepositoryMappingException("Id Field cannot be null."));
        }

        JsonObject content = JsonObject.create();

        for (Field field : properties.fieldProperties()) {
            String name = field.getName();
            String value = properties.get(field, document, String.class);
            content.put(name, value);
        }

        return bucket
            .upsert(JsonDocument.create(id, content))
            .map(new Func1<JsonDocument, T>() {
                @Override
                public T call(JsonDocument stored) {
                    return document;
                }
            });
    }
}
