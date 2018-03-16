/**
 * Copyright (C) 2015 Couchbase, Inc.
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.java.search.fulltext;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.search.IndexParams;

/**
 * @author Sergey Avseyev
 */
public class FullTextIndexParams implements IndexParams {
    private final FullTextIndexMapping mapping;
    private final FullTextStore store;

    public FullTextIndexParams(Builder builder) {
        mapping = builder.mapping;
        store = builder.store;
    }

    public FullTextIndexMapping mapping() {
        return mapping;
    }

    public FullTextStore store() {
        return store;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("mapping", mapping.json());
        json.put("store", store.json());
        return json;
    }

    public static class Builder {

        private FullTextIndexMapping mapping = FullTextIndexMapping.builder().build();
        public FullTextStore store = FullTextStore.builder().build();

        protected Builder() {
        }

        public FullTextIndexParams build() {
            return new FullTextIndexParams(this);
        }

        public Builder mapping(FullTextIndexMapping mapping) {
            this.mapping = mapping;
            return this;
        }

        public Builder store(FullTextStore store) {
            this.store = store;
            return this;
        }
    }

}
