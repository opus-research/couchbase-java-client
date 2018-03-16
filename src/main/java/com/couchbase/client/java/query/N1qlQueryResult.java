/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.java.query;

import java.util.Iterator;
import java.util.List;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Represents the results of a {@link N1qlQuery}, in a blocking fashion.
 * Note that the result is complete, meaning it will block until all
 * data has been streamed from the server.
 *
 * @author Michael Nitschinger
 * @since 2.0.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public interface N1qlQueryResult extends Iterable<N1qlQueryRow> {

    /**
     * @return the list of all {@link N1qlQueryRow}, the results of the query, if successful.
     */
    List<N1qlQueryRow> allRows();

    /**
     * @return an iterator over the list of all {@link N1qlQueryRow}, the results of the query, if successful.
     */
    Iterator<N1qlQueryRow> rows();

    /**
     * @return an object representing the signature of the results, that can be used to
     * learn about the common structure of each {@link #rows() row}. This signature is usually a
     * {@link JsonObject}, but could also be any JSON-valid type like a boolean scalar, {@link JsonArray}...
     */
    Object signature();

    /**
     * @return an object describing some metrics/info about the execution of the query.
     */
    N1qlMetrics info();

    /**
     * @return true if the query could be parsed, false if it short-circuited due to syntax/fatal error.
     */
    boolean parseSuccess();

    /**
     * Denotes the success or failure of the query. It could fail slower than with
     * {@link #parseSuccess()}, for example if a fatal error comes up while streaming the results
     * to the client. Receiving a (single) value for finalSuccess means the query is over.
     */
    boolean finalSuccess();

    /**
     * @return A list of errors or warnings encountered while executing the query.
     */
    List<JsonObject> errors();

    /**
     * @return the requestId generated by the server
     */
    String requestId();

    /**
     * @return the clientContextId that was set by the client (could be truncated to 64 bytes of UTF-8 chars)
     */
    String clientContextId();

}