package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.subdoc.DocumentFragment;

/**
 * Subdocument exception thrown when a path already exists and it shouldn't
 * (for example, when using {@link Bucket#insertIn(DocumentFragment, boolean, PersistTo, ReplicateTo) insertIn}).
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class PathExistsException extends SubDocumentException {

    public PathExistsException(String id, String path) {
        super("Path " + path + " already exist in document " + id);
    }

    public PathExistsException(String reason) {
        super(reason);
    }
}
