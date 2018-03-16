package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Subdocument exception thrown when the targeted enclosing document itself is not JSON.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class DocumentNotJsonException extends CouchbaseException {

    public DocumentNotJsonException(String id) {
        super("Document " + id + " is not a JSON document");
    }
}
