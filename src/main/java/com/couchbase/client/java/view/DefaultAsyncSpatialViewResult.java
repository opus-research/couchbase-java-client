package com.couchbase.client.java.view;

import com.couchbase.client.java.document.json.JsonObject;
import rx.Observable;

/**
 * Created by michael on 05/05/14.
 */
public class DefaultAsyncSpatialViewResult implements AsyncSpatialViewResult {

    private final Observable<AsyncSpatialViewRow> rows;
    private final boolean success;
    private final JsonObject error;
    private final JsonObject debug;

    public DefaultAsyncSpatialViewResult(Observable<AsyncSpatialViewRow> rows, boolean success, JsonObject error, JsonObject debug) {
        this.rows = rows;
        this.success = success;
        this.error = error;
        this.debug = debug;
    }

    @Override
    public Observable<AsyncSpatialViewRow> rows() {
        return rows;
    }

    @Override
    public boolean success() {
        return success;
    }

    @Override
    public JsonObject error() {
        return error;
    }

    @Override
    public JsonObject debug() {
        return debug;
    }
}
