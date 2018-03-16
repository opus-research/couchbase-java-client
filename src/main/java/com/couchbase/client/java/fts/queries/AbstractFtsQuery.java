/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.fts.queries;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.SearchParams;

public abstract class AbstractFtsQuery {

    private Double boost;

    protected AbstractFtsQuery() { }

    public AbstractFtsQuery boost(double boost) {
        this.boost = boost;
        return this;
    }

    public JsonObject export() {
        return export(null);
    }

    public JsonObject export(SearchParams searchParams) {
        JsonObject result = JsonObject.create();
        if (searchParams != null) {
            searchParams.injectParams(result);
        }
        JsonObject query = JsonObject.create();
        injectParamsAndBoost(query);
        return result.put("query", query);
    }

    public void injectParamsAndBoost(JsonObject input) {
        if (boost != null) {
            input.put("boost", boost);
        }
        injectParams(input);
    }

    protected abstract void injectParams(JsonObject input);

    @Override
    public String toString() {
        return export(null).toString();
    }

}
