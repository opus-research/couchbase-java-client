package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Subdocument exception thrown when existing number value in document is too big.
 *
 * The acceptable range on the server side is between C's INT64_MIN and INT64_MAX.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class NumberTooBigException extends CouchbaseException {
}
