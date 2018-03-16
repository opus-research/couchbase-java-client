package com.couchbase.client.java.view;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.util.Blocking;
import rx.Observable;
import rx.functions.Func1;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class DefaultSpatialViewResult implements SpatialViewResult {

    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
    private AsyncSpatialViewResult asyncViewResult;
    private final long timeout;
    private final CouchbaseEnvironment env;
    private final Bucket bucket;

    public DefaultSpatialViewResult(CouchbaseEnvironment env, Bucket bucket, Observable<AsyncSpatialViewRow> rows, boolean success, JsonObject error, JsonObject debug) {
        asyncViewResult = new DefaultAsyncSpatialViewResult(rows, success, error, debug);
        this.timeout = env.viewTimeout();
        this.env = env;
        this.bucket = bucket;
    }

    @Override
    public List<SpatialViewRow> allRows() {
        return allRows(timeout, TIMEOUT_UNIT);
    }

    @Override
    public List<SpatialViewRow> allRows(long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncViewResult
            .rows()
            .map(new Func1<AsyncSpatialViewRow, SpatialViewRow>() {
                @Override
                public SpatialViewRow call(AsyncSpatialViewRow asyncViewRow) {
                    return new DefaultSpatialViewRow(env, bucket, asyncViewRow.id(), asyncViewRow.key(), asyncViewRow.value(), asyncViewRow.geometry());
                }
            })
            .toList(), timeout, timeUnit);
    }

    @Override
    public Iterator<SpatialViewRow> rows() {
        return rows(timeout, TIMEOUT_UNIT);
    }

    @Override
    public Iterator<SpatialViewRow> rows(long timeout, TimeUnit timeUnit) {
        return asyncViewResult
            .rows()
            .map(new Func1<AsyncSpatialViewRow, SpatialViewRow>() {
                @Override
                public SpatialViewRow call(AsyncSpatialViewRow asyncViewRow) {
                    return new DefaultSpatialViewRow(env, bucket, asyncViewRow.id(), asyncViewRow.key(), asyncViewRow.value(), asyncViewRow.geometry());
                }
            })
            .timeout(timeout, timeUnit)
            .toBlocking()
            .getIterator();
    }

    @Override
    public boolean success() {
        return asyncViewResult.success();
    }

    @Override
    public JsonObject error() {
        return asyncViewResult.error();
    }

    @Override
    public JsonObject debug() {
        return asyncViewResult.debug();
    }

    @Override
    public Iterator<SpatialViewRow> iterator() {
        return rows();
    }
}
