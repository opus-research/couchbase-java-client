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

import javax.management.Query;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.SearchParams;
import com.couchbase.client.java.fts.SearchQuery;
import org.junit.Test;

public class PrefixQueryTest {

    @Test
    public void shouldExportPrefixQuery() {
        PrefixQuery query = SearchQuery.prefix("someterm");
        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create().put("prefix", "someterm"));
        assertEquals(expected, query.export());
    }

    @Test
    public void shouldExportPrefixQueryWithAllOptions() {
        SearchParams params = SearchParams.build().explain();
        PrefixQuery query = SearchQuery.prefix("someterm")
            .field("field")
            .boost(1.5);

        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create()
                .put("prefix", "someterm")
                .put("boost", 1.5)
                .put("field", "field"))
            .put("explain", true);
        assertEquals(expected, query.export(params));
    }

}