package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * An abstract common class for all {@link CouchbaseException} that relates
 * to the sub-document feature.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public abstract class SubDocumentException extends CouchbaseException {

    protected SubDocumentException() {
    }

    protected SubDocumentException(String message) {
        super(message);
    }

    protected SubDocumentException(String message, Throwable cause) {
        super(message, cause);
    }

    protected SubDocumentException(Throwable cause) {
        super(cause);
    }
}
