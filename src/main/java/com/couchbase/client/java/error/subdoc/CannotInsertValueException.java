package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.error.TranscodingException;

/**
 * Subdocument exception thrown when the provided value cannot be inserted at the given path.
 *
 * Note that since the SDK serializes data to JSON beforehand, this cannot happen because value is invalid JSON
 * (a {@link TranscodingException} would be thrown instead in this case).
 *
 * @author Simon Baslé
 * @since 2.2
 */
public class CannotInsertValueException extends CouchbaseException {

    public CannotInsertValueException(String reason) {
        super(reason);
    }
}
