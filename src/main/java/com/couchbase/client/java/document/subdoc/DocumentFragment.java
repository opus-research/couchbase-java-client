package com.couchbase.client.java.document.subdoc;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * A fragment of a {@link JsonDocument JSON Document} that can be any JSON value (including String, {@link JsonObject},
 * {@link JsonArray}, etc...), as returned and used in the sub-document API.
 *
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class DocumentFragment<T> {

    private String id;
    private String path;
    private T fragment;
    private long cas;
    private int expiry;
    private MutationToken mutationToken;

    public DocumentFragment(String id, String path, T fragment, int expiry, long cas,
            MutationToken mutationToken) {
        this.id = id;
        this.path = path;
        this.fragment = fragment;
        this.cas = cas;
        this.expiry = expiry;
        this.mutationToken = mutationToken;
    }

    public DocumentFragment(String id, String path, T fragment, int expiry) {
        this.id = id;
        this.path = path;
        this.fragment = fragment;
        this.expiry = expiry;
    }

    public DocumentFragment(String id, String path, T fragment) {
        this.id = id;
        this.path = path;
        this.fragment = fragment;
    }

    public String id() {
        return this.id;
    }

    public String path() {
         return this.path;
    }

    public T fragment() {
       return this.fragment;
    }

    public long cas() {
        return this.cas;
    }

    public int expiry() {
        return this.expiry;
    }

    public MutationToken mutationToken() {
        return this.mutationToken;
    }

}