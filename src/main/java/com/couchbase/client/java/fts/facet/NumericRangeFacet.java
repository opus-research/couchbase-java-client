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

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class NumericRangeFacet extends SearchFacet {

    private final NumericRange[] numericRanges;

    NumericRangeFacet(String name, String field, int limit, NumericRange[] numericRanges) {
        super(name, field, limit);
        this.numericRanges = numericRanges;
    }

    @Override
    public void injectParams(JsonObject queryJson) {
        super.injectParams(queryJson);

        JsonArray numericRange = JsonArray.empty();
        for (NumericRange nr : numericRanges) {
            numericRange.add(JsonObject.create()
                .put("name", nr.name())
                .put("min", nr.min())
                .put("max", nr.max())
            );
        }
        queryJson.put("numeric_ranges", numericRange);
    }
}
