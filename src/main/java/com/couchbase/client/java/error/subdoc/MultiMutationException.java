package com.couchbase.client.java.error.subdoc;

import java.util.List;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.java.document.subdoc.MutationSpec;

/**
 * Exception denoting that at least one error occurred when applying
 * multiple mutations using the sub-document API. None of the mutations were applied.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class MultiMutationException extends SubDocumentException {

    private final int index;
    private final ResponseStatus status;
    private final List<MutationSpec> originalSpec;

    public MultiMutationException(int index, ResponseStatus errorStatus, List<MutationSpec> originalSpec,
            CouchbaseException errorException) {
        super("Multiple mutation could not be applied. First problematic failure at " + index
                + " with status " + errorStatus, errorException);
        this.index = index;
        this.status = errorStatus;
        this.originalSpec = originalSpec;
    }

    /**
     * @return the zero-based index of the first mutation spec that caused the multi mutation to fail.
     */
    public int firstFailureIndex() {
        return index;
    }

    /**
     * @return the error status for the first mutation spec that caused the multi mutation to fail.
     */
    public ResponseStatus firstFailureStatus() {
        return status;
    }

    /**
     * @return the list of {@link MutationSpec} that was originally passed to the multi-mutation operation.
     */
    public List<MutationSpec> originalSpec() {
        return originalSpec;
    }

    /**
     * @return the mutation spec in {@link #originalSpec()} at index {@link #firstFailureIndex()}.
     */
    public MutationSpec firstFailureSpec() {
        return originalSpec.get(index);
    }
}
