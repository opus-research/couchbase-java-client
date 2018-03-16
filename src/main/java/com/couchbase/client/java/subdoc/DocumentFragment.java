/*
 * Copyright (C) 2016 Couchbase, Inc.
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

package com.couchbase.client.java.subdoc;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * A fragment of a {@link JsonDocument JSON Document}, that is to say one or several JSON values from the document
 * (including String, {@link JsonObject}, {@link JsonArray}, etc...), as returned and used in the sub-document API.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 2.2
 * @param <OPERATION> the broad type of subdocument operation, either {@link Lookup} or {@link Mutation}.
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class DocumentFragment<OPERATION> {

    private final String id;
    private final long cas;
    private final MutationToken mutationToken;

    private final List<MultiResult<OPERATION>> resultList;

    public DocumentFragment(String id, long cas, MutationToken mutationToken, List<MultiResult<OPERATION>> resultList) {
        this.id = id;
        this.cas = cas;
        this.mutationToken = mutationToken;

        this.resultList = resultList;
    }

    /**
     * @return the {@link JsonDocument#id() id} of the enclosing JSON document in which this fragment belongs.
     */
    public String id() {
        return this.id;
    }

    /**
     * The CAS (Create-and-Set) is set by the SDK when mutating, reflecting the new CAS from the enclosing JSON document.
     *
     * @return the CAS value related to the enclosing JSON document.
     */
    public long cas() {
        return this.cas;
    }

    /**
     * @return the updated {@link MutationToken} related to the enclosing JSON document after a mutation.
     */
    public MutationToken mutationToken() {
        return this.mutationToken;
    }

    public int size() {
        return resultList.size();
    }

    public <T> T content(String path, Class<T> targetClass) {
        if (path == null) {
            return null;
        }
        for (MultiResult<OPERATION> result : resultList) {
            if (path.equals(result.path())) {
                return interpretResult(result);
            }
        }
        return null;
    }

    public Object content(String path) {
        return this.content(path, Object.class);
    }

    public <T> T content(int index, Class<T> targetClass) {
        return interpretResult(resultList.get(index));
    }

    private <T> T interpretResult(MultiResult<OPERATION> result) {
        if (result.status() == ResponseStatus.FAILURE && result.value() instanceof RuntimeException) {
            //case where a fatal error happened while PARSING the response
            throw (RuntimeException) result.value();
        } else if (result.value() instanceof CouchbaseException) {
            //case where the server returned an error for this operation
            throw (CouchbaseException) result.value();
        } else {
            //case where the server returned a value (or null if not applicable) for this operation
            return (T) result.value();
        }
    }

    public Object content(int index) {
        return this.content(index, Object.class);
    }

    public ResponseStatus status(String path) {
        if (path == null) {
            return null;
        }
        for (MultiResult<OPERATION> result : resultList) {
            if (path.equals(result.path())) {
                return result.status();
            }
        }
        return null;
    }

    public ResponseStatus status(int index) {
        return resultList.get(index).status();
    }

    public boolean exists(String path) {
        if (path == null) {
            return false;
        }
        for (MultiResult<OPERATION> result : resultList) {
            if (path.equals(result.path())) {
                return result.exists();
            }
        }
        return false;
    }

    public boolean exists(int specIndex) {
        return specIndex >= 0 && specIndex < resultList.size()
                && resultList.get(specIndex).exists();
    }

    @Override
    public String toString() {
        //TODO show results
        return "DocumentFragment{" +
                "id='" + id + '\'' +
                ", cas=" + cas +
                ", mutationToken=" + mutationToken +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DocumentFragment<?> that = (DocumentFragment<?>) o;

        if (cas != that.cas) {
            return false;
        }
        if (!id.equals(that.id)) {
            return false;
        }
        if (mutationToken != null ? !mutationToken.equals(that.mutationToken) : that.mutationToken != null) {
            return false;
        }
        return resultList.equals(that.resultList);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (int) (cas ^ (cas >>> 32));
        result = 31 * result + (mutationToken != null ? mutationToken.hashCode() : 0);
        result = 31 * result + resultList.hashCode();
        return result;
    }
}