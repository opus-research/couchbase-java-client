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

public class DateRangeFacet extends SearchFacet {

    private final DateRange[] dateRanges;

    DateRangeFacet(String name, String field, int limit, DateRange[] dateRanges) {
        super(name, field, limit);
        this.dateRanges = dateRanges;
    }

    @Override
    public void injectParams(JsonObject queryJson) {
        super.injectParams(queryJson);

        JsonArray dateRange = JsonArray.empty();
        for (DateRange dr : dateRanges) {
            JsonObject drJson = JsonObject.create();
            drJson.put("name", dr.name());

            if (dr.start() != null) {
                drJson.put("start", dr.start());
            }
            if (dr.end() != null) {
                drJson.put("end", dr.end());
            }

            dateRange.add(drJson);
        }
        queryJson.put("date_ranges", dateRange);
    }
}
