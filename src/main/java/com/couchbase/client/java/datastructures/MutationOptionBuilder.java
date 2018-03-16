package com.couchbase.client.java.datastructures;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;

/**
 * MutationOptionBuilder allows to set following constraints on data structure mutation operations
 * - cas
 * - expiry
 * - persistence
 * - replication
 *
 * @author subhashni
 */

@InterfaceStability.Experimental
@InterfaceAudience.Public
public class MutationOptionBuilder {

    protected int expiry;
    protected long cas;
    protected PersistTo persistTo;
    protected ReplicateTo replicateTo;

    private MutationOptionBuilder() {
        this.expiry = 0;
        this.cas = 0L;
        this.persistTo = PersistTo.NONE;
        this.replicateTo = ReplicateTo.NONE;
    }

    public static MutationOptionBuilder build() {
        return new MutationOptionBuilder();
    }

    /**
     * Apply expiration for the whole document backing the data structure
     *
     * @param expiry expiration time, 0 means no expiry
     */
    public void withExpiry(int expiry) {
        this.expiry = expiry;
    }

    /**
     * Use optimistic locking for the data structure mutation
     *
     * @param cas the CAS to compare the enclosing document to.
     */
    public void withCAS(long cas) {
        this.cas = cas;
    }

    /**
     * Persistence durability constraints for the mutation
     *
     * @param persistTo the persistence durability constraint to observe.
     */
    public void withDurability(PersistTo persistTo) {
        this.persistTo = persistTo;
    }

    /**
     * Replication durability constraints for the mutation
     *
     * @param replicateTo the replication durability constraint to observe.
     */
    public void withDurability(ReplicateTo replicateTo) {
        this.replicateTo = replicateTo;
    }

    /**
     * Persistence and replication durability constraints for the mutation
     *
     * @param persistTo the persistence durability constraint to observe.
     * @param replicateTo the replication durability constraint to observe.
     */

    public void withDurability(PersistTo persistTo, ReplicateTo replicateTo) {
        this.persistTo = persistTo;
        this.replicateTo = replicateTo;
    }
}