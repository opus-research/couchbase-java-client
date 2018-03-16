package com.couchbase.client.java.document.subdoc;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.LookupCommand;
import com.couchbase.client.core.message.kv.subdoc.multi.MutationCommand;

/**
 * Utility class to create specs for the sub-document API's multi-{@link LookupCommand lookup} operations.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class LookupSpec extends LookupCommand {

    LookupSpec(Lookup type, String path) {
        super(type, path);
    }

    /**
     * Create a GET lookup specification.
     */
    public static LookupSpec get(String path) {
        return new LookupSpec(Lookup.GET, path);
    }

    /**
     * Create an EXIST lookup specification.
     */
    public static LookupSpec exists(String path) {
        return new LookupSpec(Lookup.EXIST, path);
    }
}
