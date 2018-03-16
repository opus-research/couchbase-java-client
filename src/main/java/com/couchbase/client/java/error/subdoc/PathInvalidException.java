package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Subdocument exception thrown when path has a syntax error, or path syntax is incorrect for the operation
 * (for example, if operation requires an array index).
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class PathInvalidException extends CouchbaseException {

    public PathInvalidException(String id, String path) {
        super("Path " + path + " is malformed or invalid in " + id);
    }

    public PathInvalidException(String reason) {
        super(reason);
    }
}
