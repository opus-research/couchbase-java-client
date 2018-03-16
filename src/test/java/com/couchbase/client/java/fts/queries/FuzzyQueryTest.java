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