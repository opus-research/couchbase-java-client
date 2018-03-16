package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Subdocument exception thrown when a path does not exist in the document.
 * The exact meaning of path existence depends on the operation and inputs.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class PathNotFoundException extends CouchbaseException {

    public PathNotFoundException(String id, String path) {
        super("Path " + path + " not found in document " + id);
    }

    public PathNotFoundException(String reason) {
        super(reason);
    }
}
