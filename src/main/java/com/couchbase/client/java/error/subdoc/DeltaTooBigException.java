package com.couchbase.client.java.error.subdoc;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Subdocument exception thrown when the delta in an arithmetic operation (eg counter)
 * would result in an out-of-range number (over {@link Long#MAX_VALUE} or under {@link Long#MIN_VALUE}).
 *
 * Note that the server also returns the corresponding error code when the delta value itself is too big,
 * but since the SDK enforces deltas to be of type long, this case shouldn't come up.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class DeltaTooBigException extends CouchbaseException {
}
