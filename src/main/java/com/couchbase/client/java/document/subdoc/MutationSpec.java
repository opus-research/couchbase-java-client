package com.couchbase.client.java.document.subdoc;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.core.message.kv.subdoc.multi.MutationCommand;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;

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

    /**
     * @return the {@link Mutation type} of the mutation.
     */
    public Mutation type() {
        return type;
    }

    /**
     * @return the path targeted by the mutation.
     */
    public String path() {
        return path;
    }

    /**
     * @return the fragment value to apply as a mutation.
     */
    public T fragment() {
        return fragment;
    }

    /**
     * @return true should the mutation create missing intermediary elements in the path (if it supports it).
     */
    public boolean createParents() {return createParents;}

    /**
     * Creates a {@link MutationSpec} to replace an existing value by the fragment.
     *
     * @see Bucket#replaceIn(DocumentFragment, PersistTo, ReplicateTo)
     */
    public static <T> MutationSpec replace(String path, T fragment) {
        return new MutationSpec<T>(Mutation.REPLACE, path, fragment, false);
    }

    /**
     * Creates a {@link MutationSpec} to insert a fragment, replacing the old value if the path exists.
     *
     * @param createParents in case elements of the path other than the last one don't exist,
     *                      set to true to create them.
     * @see Bucket#upsertIn(DocumentFragment, boolean, PersistTo, ReplicateTo)
     */
    public static <T> MutationSpec upsert(String path, T fragment, boolean createParents) {
        return new MutationSpec<T>(Mutation.DICT_UPSERT, path, fragment, createParents);
    }

    /**
     * Creates a {@link MutationSpec} to insert a fragment provided the last element of the path doesn't exists.
     *
     * @param createParents in case elements of the path other than the last one don't exist,
     *                      set to true to create them.
     * @see Bucket#insertIn(DocumentFragment, boolean, PersistTo, ReplicateTo)
     */
    public static <T> MutationSpec insert(String path, T fragment, boolean createParents) {
        return new MutationSpec<T>(Mutation.DICT_ADD, path, fragment, createParents);
    }

    /**
     * Creates a {@link MutationSpec} to extend an existing array, prepending or appending the value
     * depending on the given direction.
     *
     * @param direction the position at which to extend the array.
     * @param createParents in case elements of the path other than the last one don't exist,
     *                      set to true to create them.
     * @see Bucket#extendIn(DocumentFragment, ExtendDirection, boolean, PersistTo, ReplicateTo)
     */
    public static <T> MutationSpec extend(String path, T value, ExtendDirection direction, boolean createParents) {
        if (direction == ExtendDirection.FRONT) {
            return new MutationSpec<T>(Mutation.ARRAY_PUSH_FIRST, path, value, createParents);
        }
        return new MutationSpec<T>(Mutation.ARRAY_PUSH_LAST, path, value, createParents);
    }

    /**
     * Creates a {@link MutationSpec} to insert into an existing array at a specific position
     * (denoted in the path, eg. "sub.array[2]").
     *
     * @see Bucket#arrayInsertIn(DocumentFragment, PersistTo, ReplicateTo)
     */
    public static <T> MutationSpec arrayInsert(String path, T value) {
        return new MutationSpec<T>(Mutation.ARRAY_INSERT, path, value, false);
    }

    /**
     * Creates a {@link MutationSpec} to insert a value in an existing array only if the value
     * isn't already contained in the array (by way of string comparison).
     *
     * @see Bucket#addUniqueIn(DocumentFragment, boolean, PersistTo, ReplicateTo)
     */
    public static <T> MutationSpec addUnique(String path, T value) {
        return new MutationSpec<T>(Mutation.ARRAY_ADD_UNIQUE, path, value, false);
    }

    /**
     * Creates a {@link MutationSpec} to increment/decrement a numerical fragment in a JSON document.
     * If the value (last element of the path) doesn't exist the counter is created and takes the value of the delta.
     *
     * @param delta the value to increment or decrement the counter by.
     */
    public static MutationSpec<Long> counter(String path, long delta) {
        return new MutationSpec<Long>(Mutation.COUNTER, path, delta, false);
    }

    /**
     * Creates a {@link MutationSpec} to remove an entry in a JSON document (scalar, array element, dictionary entry,
     * whole array or dictionary, depending on the path).
     */
    public static <T> MutationSpec remove(String path) {
        return new MutationSpec<T>(Mutation.DELETE, path, null, false);
    }
}