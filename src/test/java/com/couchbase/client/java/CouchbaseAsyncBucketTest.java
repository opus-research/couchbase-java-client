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
package com.couchbase.client.java;

import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.RequestFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.ReplaceRequest;
import com.couchbase.client.core.message.kv.ReplaceResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.transcoder.Transcoder;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import rx.Observable;

import java.util.Collections;

import static com.couchbase.client.core.endpoint.ResponseStatusConverter.BINARY_SUCCESS;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies functionality of the {@link CouchbaseAsyncBucket}.
 *
 * @author Michael Nitschinger
 * @since 2.1.1
 */
public class CouchbaseAsyncBucketTest {

    @Test
    public void shouldNotCallIntoObserveOnInsertWhenNotNeeded() {
        CouchbaseCore core = mock(CouchbaseCore.class);
        CouchbaseAsyncBucket bucket = new CouchbaseAsyncBucket(
            core, null, "bucket", "", Collections.<Transcoder<? extends Document, ?>>emptyList()
        );

        when(core.send(argThat(new IsRequestInFactory(InsertRequest.class)))).thenReturn(Observable.just((CouchbaseResponse) new InsertResponse(
            ResponseStatus.SUCCESS, BINARY_SUCCESS, 1234, "bucket", Unpooled.EMPTY_BUFFER, null, mock(CouchbaseRequest.class)
        )));

        JsonDocument doc = JsonDocument.create("foo");
        Observable<JsonDocument> result = bucket.insert(doc, PersistTo.NONE, ReplicateTo.NONE);
        result.toBlocking().single();

        verify(core, times(1)).send(argThat(new IsRequestInFactory(InsertRequest.class)));
    }

    @Test
    public void shouldNotCallIntoObserveOnUpsertWhenNotNeeded() {
        CouchbaseCore core = mock(CouchbaseCore.class);
        CouchbaseAsyncBucket bucket = new CouchbaseAsyncBucket(
            core, null, "bucket", "", Collections.<Transcoder<? extends Document, ?>>emptyList()
        );

        when(core.send(argThat(new IsRequestInFactory(UpsertRequest.class)))).thenReturn(Observable.just((CouchbaseResponse) new UpsertResponse(
            ResponseStatus.SUCCESS, BINARY_SUCCESS, 1234, "bucket", Unpooled.EMPTY_BUFFER, null, mock(CouchbaseRequest.class)
        )));

        JsonDocument doc = JsonDocument.create("foo");
        Observable<JsonDocument> result = bucket.upsert(doc, PersistTo.NONE, ReplicateTo.NONE);
        result.toBlocking().single();

        verify(core, times(1)).send(argThat(new IsRequestInFactory(UpsertRequest.class)));
    }

    @Test
    public void shouldNotCallIntoObserveOnReplaceWhenNotNeeded() {
        CouchbaseCore core = mock(CouchbaseCore.class);
        CouchbaseAsyncBucket bucket = new CouchbaseAsyncBucket(
            core, null, "bucket", "", Collections.<Transcoder<? extends Document, ?>>emptyList()
        );

        when(core.send(argThat(new IsRequestInFactory(ReplaceRequest.class)))).thenReturn(Observable.just((CouchbaseResponse) new ReplaceResponse(
            ResponseStatus.SUCCESS, BINARY_SUCCESS, 1234, "bucket", Unpooled.EMPTY_BUFFER, null, mock(CouchbaseRequest.class)
        )));

        JsonDocument doc = JsonDocument.create("foo");
        Observable<JsonDocument> result = bucket.replace(doc, PersistTo.NONE, ReplicateTo.NONE);
        result.toBlocking().single();

        verify(core, times(1)).send(argThat(new IsRequestInFactory(ReplaceRequest.class)));
    }

    @Test
    public void shouldNotCallIntoObserveOnRemoveWhenNotNeeded() {
        CouchbaseCore core = mock(CouchbaseCore.class);
        CouchbaseAsyncBucket bucket = new CouchbaseAsyncBucket(
            core, null, "bucket", "", Collections.<Transcoder<? extends Document, ?>>emptyList()
        );

        when(core.send(argThat(new IsRequestInFactory(RemoveRequest.class)))).thenReturn(Observable.just((CouchbaseResponse) new RemoveResponse(
            ResponseStatus.SUCCESS, BINARY_SUCCESS, 1234, "bucket", Unpooled.EMPTY_BUFFER, null, mock(CouchbaseRequest.class)
        )));

        JsonDocument doc = JsonDocument.create("foo");
        Observable<JsonDocument> result = bucket.remove(doc, PersistTo.NONE, ReplicateTo.NONE);
        result.toBlocking().single();

        verify(core, times(1)).send(argThat(new IsRequestInFactory(RemoveRequest.class)));
    }

    class IsRequestInFactory extends ArgumentMatcher<RequestFactory> {
        private final Class<?> toMatch;

        public IsRequestInFactory(Class<?> toMatch) {
            this.toMatch = toMatch;
        }

        @Override
        public boolean matches(Object argument) {
            return toMatch.isAssignableFrom(((RequestFactory) argument).call().getClass());
        }
    }

}
