package com.couchbase.client.java.document.subdoc;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;

/**
 * A result corresponding to a single {@link LookupSpec}.
 *
 * When contained in a {@link DocumentFragment DocumentFragment<List<LookupResult>>},
 * having at least one LookupResult with {@link #isSuccess()} false should be verifiable
 * at the DocumentFragment level by having the fragment's status return {@link ResponseStatus#SUBDOC_MULTI_PATH_FAILURE}.
 *
 * @author Simon Baslé
 * @since 2.2
 */
public class LookupResult {

    private final String path;
    private final Lookup operation;

    private final ResponseStatus status;

    private final Object value;

    public LookupResult(String path, Lookup operation, ResponseStatus status, Object value) {
        this.path = path;
        this.operation = operation;
        this.status = status;
        this.value = value;
    }

    public String path() {
        return path;
    }

    public Lookup operation() {
        return operation;
    }

    public ResponseStatus status() {
        return status;
    }

    public Object value() {
        return value;
    }

    public boolean isSuccess() {
        return status.isSuccess() || (operation == Lookup.EXIST && value != null); //GET could have a null value and still be a success
    }
}
