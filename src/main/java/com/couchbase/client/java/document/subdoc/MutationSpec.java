package com.couchbase.client.java.document.subdoc;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.core.message.kv.subdoc.multi.MutationCommand;

/**
 * Utility class to create specs for the sub-document API's multi-{@link MutationCommand mutation} operations.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class MutationSpec<T> {
    private final Mutation type;
    private final String path;
    private final T fragment;
    private final boolean createParents;

    MutationSpec(Mutation type, String path, T fragment, boolean createParents) {
        this.type = type;
        this.path = path;
        this.fragment = fragment;
        this.createParents = createParents;
    }

    public Mutation type() {return type;}
    public String path() {return path;}
    public T fragment() {return fragment;}
    public boolean createParents() {return createParents;}


    public static <T> MutationSpec replace(String path, T fragment) {
        return new MutationSpec<T>(Mutation.REPLACE, path, fragment, false);
    }

    public static <T> MutationSpec upsert(String path, T fragment, boolean createParents) {
        return new MutationSpec<T>(Mutation.DICT_UPSERT, path, fragment, createParents);
    }
}