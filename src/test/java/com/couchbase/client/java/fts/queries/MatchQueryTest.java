/**
 * Copyright (C) 2016 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.java.fts.queries;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.SearchParams;
import com.couchbase.client.java.fts.SearchQuery;
import org.junit.Test;

import static org.junit.Assert.*;

public class MatchQueryTest {

    @Test
    public void shouldExportMatchQuery() {
        MatchQuery query = SearchQuery.match("salty beers");
        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create().put("match", "salty beers"));
        assertEquals(expected, query.export());
    }

    @Test
    public void shouldExportMatchQueryWithAllOptions() {
        MatchQuery query = SearchQuery.match("salty beers", SearchParams.build().limit(10))
            .analyzer("analyzer")
            .boost(1.5)
            .field("field")
            .fuzziness(1234)
            .prefixLength(4);

        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create()
                .put("match", "salty beers")
                .put("analyzer", "analyzer")
                .put("boost", 1.5)
                .put("field", "field")
                .put("fuzziness", 1234)
                .put("prefix_length", 4))
            .put("size", 10);
        assertEquals(expected, query.export());
    }

}