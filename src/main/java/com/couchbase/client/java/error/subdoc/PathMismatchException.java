package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Subdocument exception thrown when the path structure conflicts with the document structure
 * (for example, if a path mentions foo.bar[0].baz, but foo.bar is actually a JSON object).
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class PathMismatchException extends CouchbaseException {

    public PathMismatchException(String id, String path) {
        super("Path mismatch \"" + path + "\" in " + id);
    }

    public PathMismatchException(String reason) {
        super(reason);
    }
}
