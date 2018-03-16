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

import static org.junit.Assert.assertEquals;

import com.couchbase.client.java.document.json.JsonNull;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.SearchParams;
import com.couchbase.client.java.fts.SearchQuery;
import org.junit.Test;

public class MatchNoneQueryTest {

    @Test
    public void shouldExportMatchNoneQuery() throws Exception {
        MatchNoneQuery query = SearchQuery.matchNone();

        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create()
                .put("match_none", JsonNull.INSTANCE));
        assertEquals(expected, query.export());
    }

    @Test
    public void shouldExportMatchNoneQueryWithAllOptions() {
        SearchParams params = SearchParams.build().limit(10);
        MatchNoneQuery query = SearchQuery.matchNone()
            .boost(1.5);

        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create()
                .put("match_none", JsonNull.INSTANCE)
                .put("boost", 1.5))
            .put("size", 10);
        assertEquals(expected, query.export(params));
    }
}