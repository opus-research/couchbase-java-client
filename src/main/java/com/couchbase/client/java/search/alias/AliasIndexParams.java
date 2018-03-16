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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sergey Avseyev
 */
public class AliasIndexParams implements IndexParams {
    public Map<String, String> targets;

    public AliasIndexParams(Builder builder) {
        targets = builder.targets;
    }

    public static Builder builder() {
        return new Builder();
    }

    public JsonObject json() {
        JsonObject json = JsonObject.create();
        JsonObject targetsJson = JsonObject.create();
        if (targets != null && !targets.isEmpty()) {
            for (Map.Entry<String, String> entry : targets.entrySet()) {
                JsonObject uuid = JsonObject.create();
                uuid.put("indexUUID", entry.getValue());
                targetsJson.put(entry.getKey(), uuid);
            }
        }
        json.put("targets", targetsJson);
        return json;
    }

    public static class Builder {

        public Map<String, String> targets = new HashMap<String, String>();

        protected Builder() {
        }

        public AliasIndexParams build() {
            return new AliasIndexParams(this);
        }

        public Builder targets(Map<String, String> targets) {
            this.targets = targets;
            return this;
        }
        public Builder target(String indexName, String uuid) {
            targets.put(indexName, uuid);
            return this;
        }

        public Builder target(String indexName) {
            targets.put(indexName, "");
            return this;
        }

    }

}
