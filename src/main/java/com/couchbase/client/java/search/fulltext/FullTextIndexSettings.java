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
import com.couchbase.client.java.search.IndexSettings;
import com.couchbase.client.java.search.PlanParams;
import com.couchbase.client.java.search.SourceParams;

/**
 * @author Sergey Avseyev
 */
public class FullTextIndexSettings implements IndexSettings {
    public static final String TYPE = "bleve";
    private final String name;
    private final FullTextIndexParams params;
    private final String sourceType;
    private final String sourceName;
    private final String sourceUUID;
    private final SourceParams sourceParams;
    private final PlanParams planParams;

    public FullTextIndexSettings(Builder builder) {
        name = builder.name;
        params = builder.params;
        sourceType = builder.sourceType;
        sourceName = builder.sourceName;
        sourceUUID = builder.sourceUUID;
        sourceParams = builder.sourceParams;
        planParams = builder.planParams;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String name() {
        return name;
    }

    public String type() {
        return TYPE;
    }

    public IndexParams params() {
        return params;
    }

    public String sourceType() {
        return sourceType;
    }

    public String sourceName() {
        return sourceName;
    }

    public String sourceUUID() {
        return sourceUUID;
    }

    public SourceParams sourceParams() {
        return sourceParams;
    }

    public PlanParams planParams() {
        return planParams;
    }

    @Override
    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("name", name);
        json.put("type", TYPE);
        json.put("params", params.json().toString());
        json.put("sourceType", sourceType);
        json.put("sourceName", sourceName);
        json.put("sourceUUID", sourceUUID);
        json.put("sourceParams", sourceParams.json().toString());
        json.put("planParams", planParams.json());
        return json;
    }

    public static class Builder {

        public FullTextIndexParams params = FullTextIndexParams.builder().build();
        public PlanParams planParams = PlanParams.builder().build();
        public String sourceType;
        public String sourceName;
        public SourceParams sourceParams;
        public String sourceUUID;
        public String name;


        protected Builder() {
        }

        public FullTextIndexSettings build() {
            return new FullTextIndexSettings(this);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder params(FullTextIndexParams params) {
            this.params = params;
            return this;
        }

        public Builder planParams(PlanParams planParams) {
            this.planParams = planParams;
            return this;
        }

        public Builder sourceType(String sourceType) {
            this.sourceType = sourceType;
            return this;
        }

        public Builder sourceName(String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public Builder sourceUUID(String sourceUUID) {
            this.sourceUUID = sourceUUID;
            return this;
        }

        public Builder sourceParams(SourceParams sourceParams) {
            this.sourceParams = sourceParams;
            return this;
        }
    }
}