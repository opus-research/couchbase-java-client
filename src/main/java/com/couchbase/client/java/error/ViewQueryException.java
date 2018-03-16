package com.couchbase.client.java.error;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.view.ViewResult;

/**
 * @author Sergey Avseyev
 */
public class ViewQueryException extends CouchbaseException {
    private final String error;
    private final String reason;

    public ViewQueryException(String error, String reason) {
        super(error + ": " + reason);
        this.error = error;
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }

    public String getError() {
        return error;
    }
}
