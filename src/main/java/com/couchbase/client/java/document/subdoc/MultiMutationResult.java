package com.couchbase.client.java.document.subdoc;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.kv.MutationToken;

/**
 * Represent a successful multi-mutation. This object allows to retrieve the CAS
 * and, if available, the {@link MutationToken} of the mutated document, post-mutation.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class MultiMutationResult {

    private final String docId;
    private final long cas;
    private final MutationToken mutationToken;

    public MultiMutationResult(String docId, long cas, MutationToken mutationToken) {
        this.docId = docId;
        this.cas = cas;
        this.mutationToken = mutationToken;
    }

    public String id() {
        return docId;
    }

    public long cas() {
        return cas;
    }

    public MutationToken mutationToken() {
        return mutationToken;
    }
}
