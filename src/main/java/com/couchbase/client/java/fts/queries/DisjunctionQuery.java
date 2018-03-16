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

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class DisjunctionQuery extends AbstractCompoundQuery {

    private int min = 1;

    public DisjunctionQuery(AbstractFtsQuery... queries) {
        super(queries);
    }

    @Override
    public DisjunctionQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    public DisjunctionQuery min(int min) {
        this.min = min;
        return this;
    }

    public DisjunctionQuery or(AbstractFtsQuery... queries) {
        super.addAll(queries);
        return this;
    }

    @Override
    protected void injectParams(JsonObject input) {
        if (childQueries.isEmpty()) {
            throw new IllegalArgumentException("Compound query has no child query");
        }
        if (childQueries.size() < min) {
            throw new IllegalArgumentException("Disjunction query as fewer children than the configured minimum " + min);
        }

        input.put("min", min);

        JsonArray disjuncts = JsonArray.create();
        for (AbstractFtsQuery childQuery : childQueries) {
            JsonObject childJson = JsonObject.create();
            childQuery.injectParamsAndBoost(childJson);
            disjuncts.add(childJson);
        }
        input.put("disjuncts", disjuncts);
    }
}
