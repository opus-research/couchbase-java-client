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

package com.couchbase.client.java.search.alias;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.search.IndexParams;
import com.couchbase.client.java.search.IndexSettings;

/**
 * @author Sergey Avseyev
 */
public class AliasIndexSettings implements IndexSettings {
    public static final String TYPE = "fulltext-alias";
    private final String name;
    private final AliasIndexParams params;

    public AliasIndexSettings(Builder builder) {
        name = builder.name;
        params = builder.params;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public IndexParams params() {
        return params;
    }

    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("name", name);
        json.put("type", TYPE);
        json.put("params", params.json().toString());
        json.put("sourceType", "nil");
        json.put("sourceName", "");
        json.put("sourceUUID", "");
        json.put("sourceParams", "null");
        return json;
    }

    public static class Builder {

        public AliasIndexParams params;
        public String name;

        protected Builder() {
        }

        public AliasIndexSettings build() {
            return new AliasIndexSettings(this);
        }

        public Builder params(AliasIndexParams params) {
            this.params = params;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

    }
}
