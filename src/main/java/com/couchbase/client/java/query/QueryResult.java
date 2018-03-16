package com.couchbase.client.java.query;

import java.util.Iterator;
import java.util.List;

import com.couchbase.client.java.document.json.JsonObject;

/**
 * Represents the results of a {@link Query}, in a blocking fashion.
 * Note that the result is complete, meaning it will block until all
 * data has been streamed from the server.
 *
 * @author Michael Nitschinger
 */
public interface QueryResult {

    List<QueryRow> allRows();

    Iterator<QueryRow> rows();

    JsonObject info();

    boolean parseSuccess();

    boolean finalSuccess();

    List<JsonObject> errors();

}
