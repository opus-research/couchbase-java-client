package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Subdocument exception thrown when document is too deep to parse.
 *
 * The current limitation is there to ensure a single parse does not consume too much memory (overloading the server).
 * This error is similar to other TooDeep errors, which all relate to various validation stages to ensure the server
 * does not consume too much memory when parsing a single document.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class DocumentTooDeepException extends CouchbaseException {

    public DocumentTooDeepException(String id) {
        super("JSON is too deep in document " + id);
    }
}