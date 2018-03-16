package com.couchbase.client.java.error;

import com.couchbase.client.core.CouchbaseException;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class FlushDisabledException extends CouchbaseException {

    public FlushDisabledException() {
    }

    public FlushDisabledException(String message) {
        super(message);
    }

    public FlushDisabledException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlushDisabledException(Throwable cause) {
        super(cause);
    }
}
