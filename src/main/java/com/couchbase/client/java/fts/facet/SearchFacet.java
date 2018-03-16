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
package com.couchbase.client.java.fts.facet;


import com.couchbase.client.java.document.json.JsonObject;

public abstract class SearchFacet {

    private final String name;
    private final String field;
    private final int limit;

    protected SearchFacet(String name, String field, int limit) {
        this.name = name;
        this.field = field;
        this.limit = limit;
    }

    public static SearchFacet term(String name, String field, int limit) {
        return new TermFacet(name, field, limit);
    }

    public static SearchFacet numeric(String name, String field, int limit, NumericRange... ranges) {
        return new NumericRangeFacet(name, field, limit, ranges);
    }

    public static SearchFacet date(String name, String field, int limit, DateRange... ranges) {
        return new DateRangeFacet(name, field, limit, ranges);
    }

    public String name() {
        return name;
    }

    public void injectParams(JsonObject queryJson) {
        queryJson.put("size", limit);
        queryJson.put("field", field);
    }

}
