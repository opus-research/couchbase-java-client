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
package com.couchbase.client.java.fts;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.facet.DateRange;
import com.couchbase.client.java.fts.facet.NumericRange;
import com.couchbase.client.java.fts.facet.SearchFacet;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.fts.facet.DateRange.dateRange;
import static com.couchbase.client.java.fts.facet.NumericRange.numericRange;
import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.verifyException;
import static org.junit.Assert.*;


public class SearchParamsTest {

    @Test
    public void shouldBeEmptyByDefault() {
        SearchParams p = SearchParams.build();
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.empty();
        assertEquals(expected, result);
    }

    @Test
    public void shouldInjectLimit() {
        SearchParams p = SearchParams.build().limit(10);
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.create().put("size", 10);
        assertEquals(expected, result);
    }

    @Test
    public void shouldInjectSkip() {
        SearchParams p = SearchParams.build()
            .skip(100);
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.create().put("from", 100);
        assertEquals(expected, result);
    }

    @Test
    public void shouldInjectLimitAndSkip() {
        SearchParams p = SearchParams.build()
            .limit(500)
            .skip(100);
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.create().put("from", 100).put("size", 500);
        assertEquals(expected, result);
    }

    @Test
    public void shouldInjectExplain() {
        SearchParams p = SearchParams.build()
            .explain();
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.create().put("explain", true);
        assertEquals(expected, result);
    }

    @Test
    public void shouldInjectHighlightStyle() {
        SearchParams p = SearchParams.build()
            .highlight(HighlightStyle.HTML);
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.create()
            .put("highlight", JsonObject.create().put("style", "html"));
        assertEquals(expected, result);
    }

    @Test
    public void shouldInjectHighlightStyleWithFields() {
        SearchParams p = SearchParams.build()
            .highlight(HighlightStyle.ANSI, "foo", "bar");
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.create()
            .put("highlight", JsonObject.create().put("style", "ansi").put("fields", JsonArray.from("foo", "bar")));
        assertEquals(expected, result);
    }

    @Test
    public void shouldInjectFields() {
        SearchParams p = SearchParams.build()
            .fields("foo", "bar", "baz");
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.create()
            .put("fields", JsonArray.from("foo", "bar", "baz"));
        assertEquals(expected, result);
    }

    @Test
    public void shouldInjectFacets() {
        SearchParams p = SearchParams.build()
            .addFacets(
                SearchFacet.term("term", "somefield", 10),
                SearchFacet.date("dr", "datefield", 1, dateRange("name", "start", "end")),
                SearchFacet.numeric("nr", "numfield", 99, numericRange("name2", 0.0, 99.99))
            );
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject term = JsonObject.create().put("size", 10).put("field", "somefield");
        JsonObject nr = JsonObject.create()
            .put("size", 99)
            .put("field", "numfield")
            .put("numeric_ranges", JsonArray.from(
                JsonObject.create().put("name", "name2").put("max", 99.99).put("min", 0.0))
            );
        JsonObject dr = JsonObject.create()
            .put("size", 1)
            .put("field", "datefield")
            .put("date_ranges", JsonArray.from(
                JsonObject.create().put("name", "name").put("start", "start").put("end", "end"))
            );
        JsonObject expected = JsonObject.create()
            .put("facets", JsonObject.create()
                .put("nr", nr)
                .put("dr", dr)
                .put("term", term)
            );
        assertEquals(expected, result);
    }

    @Test
    public void shouldAddFacetsToExistingFacets() {
        SearchParams p = SearchParams.build()
                .addFacets(SearchFacet.term("A", "field1", 1))
                .addFacets(SearchFacet.term("B", "field2", 2));

        JsonObject result = JsonObject.create();
        p.injectParams(result);

        JsonObject expected = JsonObject.create()
            .put("facets", JsonObject.create()
                .put("A", JsonObject.create().put("field", "field1").put("size", 1))
                .put("B", JsonObject.create().put("field", "field2").put("size", 2))
            );
        assertEquals(expected, result);
    }

    @Test
    public void shouldReplaceExistingFacetWithSameName() {
        SearchParams p = SearchParams.build()
                .addFacets(SearchFacet.term("A", "field1", 1))
                .addFacets(SearchFacet.term("A", "field2", 2));

        JsonObject result = JsonObject.create();
        p.injectParams(result);

        JsonObject expected = JsonObject.create()
            .put("facets", JsonObject.create()
                    .put("A", JsonObject.create()
                            .put("field", "field2").put("size", 2))
            );
        assertEquals(expected, result);
    }

    @Test
    public void shouldClearExistingFacets() {
        SearchParams p = SearchParams.build()
                .addFacets(SearchFacet.term("A", "field1", 1))
                .clearFacets()
                .addFacets(SearchFacet.term("B", "field2", 2));

        JsonObject result = JsonObject.create();
        p.injectParams(result);

        JsonObject expected = JsonObject.create()
            .put("facets", JsonObject.create()
                .put("B", JsonObject.create().put("field", "field2").put("size", 2))
            );
        assertEquals(expected, result);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowOnDateRangeWithoutName() {
        DateRange.dateRange(null, "a", "b");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowOnDateRangeWithoutBoundaries() {
        DateRange.dateRange("name", null, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowOnNumericRangeWithoutName() {
        NumericRange.numericRange(null, 1.2, 3.4);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowOnNumericRangeWithoutBoundaries() {
        NumericRange.numericRange("name", null, null);
    }

    @Test
    public void shouldAllowOneNullBoundOnDateRange() {
        DateRange.dateRange("name", "a", null);
        DateRange.dateRange("name", null, "b");
    }

    @Test
    public void shouldAllowOneNullBoundOnNumericRange() {
        NumericRange.numericRange("name", 1.2, null);
        NumericRange.numericRange("name", null, 3.4);
    }

    @Test
    public void shouldInjectServerSideTimeout() {
        SearchParams p = SearchParams.build()
            .serverSideTimeout(3, TimeUnit.SECONDS);
        JsonObject result = JsonObject.empty();
        p.injectParams(result);

        JsonObject expected = JsonObject.create()
            .put("ctl", JsonObject.create().put("timeout", 3000L));
        assertEquals(expected, result);
    }

}