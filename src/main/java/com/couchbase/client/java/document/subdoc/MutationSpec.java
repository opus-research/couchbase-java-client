package com.couchbase.client.java.document.subdoc;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.kv.subdoc.multi.LookupCommand;
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
    private final MutationSpecType type;
    private final String path;
    private final T fragment;
    private final boolean createParents;
    private final ExtendDirection direction;
    private final long delta;

    MutationSpec(MutationSpecType type, String path, T fragment, boolean createParents, ExtendDirection direction, long delta) {
        this.type = type;
        this.path = path;
        this.fragment = fragment;
        this.createParents = createParents;
        this.direction = direction;
        this.delta = delta;
    }

    public MutationSpecType type() {return type;}
    public String path() {return path;}
    public T fragment() {return fragment;}
    public boolean createParents() {return createParents;}
    public ExtendDirection direction() {return direction;}
    public long delta() {return delta;}

    private enum MutationSpecType { REPLACE, UPSERT, INSERT, EXTEND, ARRAY_INSERT, ADD_UNIQUE, COUNTER, REMOVE }

    public static <T> MutationSpec replace(String path, T fragment) {
        return new MutationSpec<T>(MutationSpecType.REPLACE, path, fragment, false, null, 0);
    }

    public static <T> MutationSpec upsert(String path, T fragment, boolean createParents) {
        return new MutationSpec<T>(MutationSpecType.UPSERT, path, fragment, createParents, null, 0);
    }
}