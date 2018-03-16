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
public interface IDocumentFragment<T> {

    /**
     * @return the {@link JsonDocument#id() id} of the enclosing JSON document in which this fragment belongs.
     */
    String id();

    /**
     * @return the path inside the enclosing JSON document at which this fragment is found, or is to be mutated.
     */
    String path();

    /**
     * @return the fragment, the value either found at the path or the target value of the mutation at the path.
     */
    T fragment();

    /**
     * The CAS (Create-and-Set) value can either be set by the user when the DocumentFragment is the input for
     * a mutation, or by the SDK when it is the return value of a subdoc operation.
     *
     * When the fragment is used as an input parameter to a mutation, the CAS will be compared to the one of
     * the {@link #id() target enclosing JSON document} and mutation only applied if both CAS match.
     *
     * When the fragment is the return value of the operation, the CAS is populated with the one from the enclosing
     * JSON document.
     *
     * @return the CAS value related to the enclosing JSON document.
     */
    long cas();

    /**
     * @return the expiry to be applied to the enclosing JSON document along a mutation.
     */
    int expiry();

    /**
     * @return the updated {@link MutationToken} related to the enclosing JSON document after a mutation
     * (when the fragment is the return value of said mutation).
     */
    MutationToken mutationToken();
}
