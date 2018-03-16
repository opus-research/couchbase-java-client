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

import static com.couchbase.client.core.endpoint.ResponseStatusConverter.BINARY_SUCCESS;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.CouchbaseCore;
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
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.AsyncQueryRow;
import com.couchbase.client.java.query.PrepareStatement;
import com.couchbase.client.java.query.PreparedPayload;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.transcoder.Transcoder;
import org.junit.Test;
import rx.Observable;

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

        when(core.send(any(InsertRequest.class))).thenReturn(Observable.just((CouchbaseResponse) new InsertResponse(
            ResponseStatus.SUCCESS, BINARY_SUCCESS, 1234, "bucket", Unpooled.EMPTY_BUFFER, null, mock(CouchbaseRequest.class)
        )));

        JsonDocument doc = JsonDocument.create("foo");
        Observable<JsonDocument> result = bucket.insert(doc, PersistTo.NONE, ReplicateTo.NONE);
        result.toBlocking().single();

        verify(core, times(1)).send(any(CouchbaseRequest.class));
    }

    @Test
    public void shouldNotCallIntoObserveOnUpsertWhenNotNeeded() {
        CouchbaseCore core = mock(CouchbaseCore.class);
        CouchbaseAsyncBucket bucket = new CouchbaseAsyncBucket(
            core, null, "bucket", "", Collections.<Transcoder<? extends Document, ?>>emptyList()
        );

        when(core.send(any(UpsertRequest.class))).thenReturn(Observable.just((CouchbaseResponse) new UpsertResponse(
            ResponseStatus.SUCCESS, BINARY_SUCCESS, 1234, "bucket", Unpooled.EMPTY_BUFFER, null, mock(CouchbaseRequest.class)
        )));

        JsonDocument doc = JsonDocument.create("foo");
        Observable<JsonDocument> result = bucket.upsert(doc, PersistTo.NONE, ReplicateTo.NONE);
        result.toBlocking().single();

        verify(core, times(1)).send(any(CouchbaseRequest.class));
    }

    @Test
    public void shouldNotCallIntoObserveOnReplaceWhenNotNeeded() {
        CouchbaseCore core = mock(CouchbaseCore.class);
        CouchbaseAsyncBucket bucket = new CouchbaseAsyncBucket(
            core, null, "bucket", "", Collections.<Transcoder<? extends Document, ?>>emptyList()
        );

        when(core.send(any(ReplaceRequest.class))).thenReturn(Observable.just((CouchbaseResponse) new ReplaceResponse(
            ResponseStatus.SUCCESS, BINARY_SUCCESS, 1234, "bucket", Unpooled.EMPTY_BUFFER, null, mock(CouchbaseRequest.class)
        )));

        JsonDocument doc = JsonDocument.create("foo");
        Observable<JsonDocument> result = bucket.replace(doc, PersistTo.NONE, ReplicateTo.NONE);
        result.toBlocking().single();

        verify(core, times(1)).send(any(CouchbaseRequest.class));
    }

    @Test
    public void shouldNotCallIntoObserveOnRemoveWhenNotNeeded() {
        CouchbaseCore core = mock(CouchbaseCore.class);
        CouchbaseAsyncBucket bucket = new CouchbaseAsyncBucket(
            core, null, "bucket", "", Collections.<Transcoder<? extends Document, ?>>emptyList()
        );

        when(core.send(any(RemoveRequest.class))).thenReturn(Observable.just((CouchbaseResponse) new RemoveResponse(
            ResponseStatus.SUCCESS, BINARY_SUCCESS, 1234, "bucket", Unpooled.EMPTY_BUFFER, null, mock(CouchbaseRequest.class)
        )));

        JsonDocument doc = JsonDocument.create("foo");
        Observable<JsonDocument> result = bucket.remove(doc, PersistTo.NONE, ReplicateTo.NONE);
        result.toBlocking().single();

        verify(core, times(1)).send(any(CouchbaseRequest.class));
    }

    @Test
    public void shouldRetryPrepareAndQueryTwiceIfNameNotFound() {
        Statement st = select("*").from(i("beer-sample")).limit(10);
        PreparedPayload nonExistingPayload = new PreparedPayload(st, "nonExistingName");
        JsonObject error = JsonObject.create().put("code", 4040).put("msg", "nonExistingName");
        AsyncQueryResult mockResultNotFound = mock(AsyncQueryResult.class);
        when(mockResultNotFound.finalSuccess()).thenReturn(Observable.just(false));
        when(mockResultNotFound.errors()).thenReturn(Observable.just(error));

        AsyncQueryRow mockRow = mock(AsyncQueryRow.class);
        AsyncQueryResult mockResultFound = mock(AsyncQueryResult.class);
        when(mockResultFound.errors()).thenReturn(Observable.<JsonObject>empty());
        when(mockResultFound.rows()).thenReturn(Observable.just(mockRow).repeat(10));
        when(mockResultFound.finalSuccess()).thenReturn(Observable.just(true));

        CouchbaseAsyncBucket mockAsyncBucket = prepareMockForPreparedStatement(nonExistingPayload,
                mockResultNotFound, mockResultFound);

        //assert behavior when
        AsyncQueryResult result = mockAsyncBucket.query(nonExistingPayload).toBlocking().first();
        List<JsonObject> errors = result.errors().toList().toBlocking().first();
        Boolean success = result.finalSuccess().toBlocking().first();
        List<AsyncQueryRow> allRows = result.rows().toList().toBlocking().first();

        assertTrue(success);
        assertTrue(errors.isEmpty());
        assertEquals(10, allRows.size());

        verify(mockAsyncBucket, times(2)).queryRaw(anyString());
        verify(mockAsyncBucket, times(1)).prepare(any(PrepareStatement.class));
    }

    @Test
    public void shouldQueryOnceAndNotReprepareIfNameFound() {
        Statement st = select("*").from(i("beer-sample")).limit(10);
        PreparedPayload existingPayload = new PreparedPayload(st, "existingName");

        AsyncQueryRow mockRow = mock(AsyncQueryRow.class);
        AsyncQueryResult mockResultFound = mock(AsyncQueryResult.class);
        when(mockResultFound.errors()).thenReturn(Observable.<JsonObject>empty());
        when(mockResultFound.rows()).thenReturn(Observable.just(mockRow).repeat(10));
        when(mockResultFound.finalSuccess()).thenReturn(Observable.just(true));

        CouchbaseAsyncBucket mockAsyncBucket = prepareMockForPreparedStatement(existingPayload, mockResultFound);

        AsyncQueryResult result = mockAsyncBucket.query(existingPayload).toBlocking().first();
        List<JsonObject> errors = result.errors().toList().toBlocking().first();
        Boolean success = result.finalSuccess().toBlocking().first();
        List<AsyncQueryRow> allRows = result.rows().toList().toBlocking().first();

        assertTrue(success);
        assertTrue(errors.isEmpty());
        assertEquals(10, allRows.size());

        verify(mockAsyncBucket, times(1)).queryRaw(anyString());
        verify(mockAsyncBucket, never()).prepare(any(Statement.class));
    }

    private CouchbaseAsyncBucket prepareMockForPreparedStatement(PreparedPayload payload,
            AsyncQueryResult firstQueryResult, AsyncQueryResult... otherQueryResults) {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                                                              .retryStrategy(BestEffortRetryStrategy.INSTANCE).build();

        Observable<AsyncQueryResult> firstResponse = Observable.just(firstQueryResult);
        Observable<AsyncQueryResult>[] otherResponses = new Observable[otherQueryResults.length];
        for (int i = 0; i < otherQueryResults.length; i++) {
            otherResponses[i] = Observable.just(otherQueryResults[i]);
        }

        CouchbaseAsyncBucket mockAsyncBucket = mock(CouchbaseAsyncBucket.class);
        when(mockAsyncBucket.environment()).thenReturn(env);
        when(mockAsyncBucket.query(any(Statement.class))).thenCallRealMethod();
        when(mockAsyncBucket.query(any(Query.class))).thenCallRealMethod();
        when(mockAsyncBucket.queryRaw(contains("\"prepared\":\"" + payload.preparedName() + "\"")))
                .thenReturn(firstResponse, otherResponses);
        when(mockAsyncBucket.prepare(any(Statement.class))).thenReturn(Observable.just(payload));
        when(mockAsyncBucket.prepare(anyString())).thenReturn(Observable.just(payload));

        return mockAsyncBucket;
    }

}
