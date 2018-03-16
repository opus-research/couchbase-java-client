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
import com.couchbase.client.java.fts.SearchQuery;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FuzzyQueryTest {

    @Test
    public void shouldExportFuzzyQuery() {
        FuzzyQuery query = SearchQuery.fuzzy("someterm");
        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create().put("term", "someterm"));
        assertEquals(expected, query.export());
    }

    @Test
    public void shouldExportFuzzyQueryWithAllOptions() {
        FuzzyQuery query = SearchQuery.fuzzy("someterm", SearchParams.build().explain())
            .boost(1.5)
            .field("field")
            .prefixLength(23)
            .fuzziness(12);

        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create()
                .put("term", "someterm")
                .put("boost", 1.5)
                .put("field", "field")
                .put("prefix_length", 23)
                .put("fuzziness", 12))
            .put("explain", true);
        assertEquals(expected, query.export());
    }

}