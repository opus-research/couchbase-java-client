package com.couchbase.client.protocol.n1ql;

import com.couchbase.client.protocol.views.HttpOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;

public class N1qlOperation implements HttpOperation {
    @Override
    public HttpRequest getRequest() {
        return null;
    }

    @Override
    public OperationCallback getCallback() {
        return null;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean hasErrored() {
        return false;
    }

    @Override
    public boolean isTimedOut() {
        return false;
    }

    @Override
    public void cancel() {

    }

    @Override
    public void timeOut() {

    }

    @Override
    public void addAuthHeader(String auth) {

    }

    @Override
    public OperationException getException() {
        return null;
    }

    @Override
    public void handleResponse(HttpResponse response) {

    }
}
