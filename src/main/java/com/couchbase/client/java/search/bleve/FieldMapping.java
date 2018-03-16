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

package com.couchbase.client.java.search.bleve;

import com.couchbase.client.java.document.json.JsonObject;

/**
 * @author Sergey Avseyev
 */
public class FieldMapping {
    private final String name;
    private final String type;
    private final String analyzer;
    private final boolean store;
    private final boolean index;
    private final boolean includeTermVectors;
    private final boolean includeInAll;
    private final String dateFormat;

    public FieldMapping(Builder builder) {
        name = builder.name;
        type = builder.type;
        analyzer = builder.analyzer;
        store = builder.store;
        index = builder.index;
        includeTermVectors = builder.includeTermVectors;
        includeInAll = builder.includeInAll;
        dateFormat = builder.dateFormat;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public String analyzer() {
        return analyzer;
    }

    public boolean store() {
        return store;
    }

    public boolean index() {
        return index;
    }

    public boolean includeTermVectors() {
        return includeTermVectors;
    }

    public boolean includeInAll() {
        return includeInAll;
    }

    public String dateFormat() {
        return dateFormat;
    }
    
    public static Builder builder() {
        return new Builder();
    }

    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("name", name);
        json.put("type", type);
        json.put("analyzer", analyzer);
        json.put("store", store);
        json.put("index", index);
        json.put("includeTermVectors", includeTermVectors);
        json.put("includeInAll", includeInAll);
        json.put("dateFormat", dateFormat);
        return json;
    }

    public static class Builder {
        public String name;
        public String type;
        public String analyzer;
        public boolean store;
        public boolean index;
        public boolean includeTermVectors;
        public boolean includeInAll;
        public String dateFormat;

        protected Builder() {
        }

        public FieldMapping build() {
            return new FieldMapping(this);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder analyzer(String analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        public Builder store(boolean store) {
            this.store = store;
            return this;
        }

        public Builder index(boolean index) {
            this.index = index;
            return this;
        }

        public Builder includeTermVectors(boolean includeTermVectors) {
            this.includeTermVectors = includeTermVectors;
            return this;
        }

        public Builder includeInAll(boolean includeInAll) {
            this.includeInAll = includeInAll;
            return this;
        }

        public Builder dateFormat(String dateFormat) {
            this.dateFormat = dateFormat;
            return this;
        }
    }
}