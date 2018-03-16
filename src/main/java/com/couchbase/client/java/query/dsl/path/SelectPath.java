package com.couchbase.client.java.query.dsl.path;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public interface SelectPath {

    SelectResultPath select(String... selects);
}
