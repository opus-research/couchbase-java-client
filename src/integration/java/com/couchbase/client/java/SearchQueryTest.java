/*
 * Copyright (C) 2015 Couchbase, Inc.
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
package com.couchbase.client.java;

import static com.couchbase.client.java.fts.facet.SearchFacet.date;
import static com.couchbase.client.java.fts.facet.SearchFacet.numeric;
import static com.couchbase.client.java.fts.facet.SearchFacet.term;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.HighlightStyle;
import com.couchbase.client.java.fts.SearchParams;
import com.couchbase.client.java.fts.SearchQuery;
import com.couchbase.client.java.fts.result.SearchQueryResult;
import com.couchbase.client.java.fts.result.SearchQueryRow;
import com.couchbase.client.java.fts.result.facets.DateRange;
import com.couchbase.client.java.fts.result.facets.DateRangeFacetResult;
import com.couchbase.client.java.fts.result.facets.FacetResult;
import com.couchbase.client.java.fts.result.facets.NumericRange;
import com.couchbase.client.java.fts.result.facets.NumericRangeFacetResult;
import com.couchbase.client.java.fts.result.facets.TermFacetResult;
import com.couchbase.client.java.fts.result.facets.TermRange;
import com.couchbase.client.java.util.CouchbaseTestContext;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests of the Search Query / FTS features.
 *
 * @author Simon Baslé
 * @since 2.3
 */
public class SearchQueryTest {

    private static CouchbaseTestContext ctx;
    private static final String INDEX = "beer-search";

    @BeforeClass
    public static void init() throws InterruptedException {
        ctx = CouchbaseTestContext.builder()
                .bucketName("beer-sample")
                .flushOnInit(false)
                .adhoc(false)
                .build()
                .ignoreIfMissing(CouchbaseFeature.FTS_BETA);
    }

    @AfterClass
    public static void cleanup() {
        ctx.destroyBucketAndDisconnect();
    }

    @Test
    public void shouldSearchWithLimit() {
        SearchQuery query = SearchQuery.matchPhrase("hop beer");

        SearchQueryResult result = ctx.bucket()
                .query(INDEX, query, SearchParams.build().limit(3));

        assertNotNull(result);
        assertNotNull(result.metrics());
        assertTrue(result.metrics().totalHits() >= 3);
        assertFalse(result.hits().isEmpty());
        assertFalse(result.hitsOrFail().isEmpty());
        assertEquals(result.hits(), result.hitsOrFail());
        assertEquals(3, result.hits().size());
        assertTrue(result.errors().isEmpty());

        for (SearchQueryRow row : result.hits()) {
            assertNotNull(row.id());
            assertTrue(row.index().startsWith(INDEX));
            assertTrue(row.score() > 0d);
            assertEquals(JsonObject.empty(), row.explanation());
            assertTrue(row.fields().isEmpty());
            assertTrue(row.fragments().isEmpty());
            System.err.println(row + "\n");
        }
    }

    @Test
    public void shouldSearchWithFields() {
        SearchQuery query = SearchQuery.matchPhrase("hop beer");

        SearchQueryResult result = ctx.bucket()
                .query(INDEX, query, SearchParams.build()
                        .limit(3)
                        .fields("name"));

        for (SearchQueryRow row : result.hits()) {
            final Map<String, String> fields = row.fields();
            assertEquals(1, fields.size());
            assertTrue(fields.containsKey("name"));
            assertNotEquals("", fields.get("name"));
            assertNotNull(fields.get("name"));

            //sanity checks
            assertNotNull(row.id());
            assertTrue(row.index().startsWith(INDEX));
            assertTrue(row.score() > 0d);
            assertEquals(JsonObject.empty(), row.explanation());
            assertTrue(row.fragments().isEmpty());
            System.err.println(row + "\n");
        }
        //top level sanity checks
        assertNotNull(result);
        assertNotNull(result.metrics());
        assertTrue(result.metrics().totalHits() >= 3);
        assertFalse(result.hits().isEmpty());
        assertFalse(result.hitsOrFail().isEmpty());
        assertEquals(result.hits(), result.hitsOrFail());
        assertEquals(3, result.hits().size());
        assertTrue(result.errors().isEmpty());
    }

    @Test
    public void shouldSearchWithFragments() {
        SearchQuery query = SearchQuery.matchPhrase("hop beer");

        SearchQueryResult result = ctx.bucket()
                .query(INDEX, query, SearchParams.build()
                        .limit(3)
                        .highlight(HighlightStyle.HTML, "name"));

        for (SearchQueryRow row : result.hits()) {
            assertFalse(row.fragments().isEmpty());

            //sanity checks
            assertNotNull(row.id());
            assertTrue(row.index().startsWith(INDEX));
            assertTrue(row.score() > 0d);
            assertEquals(JsonObject.empty(), row.explanation());
            assertTrue(row.fields().isEmpty());
        }
        //top-level sanity checks
        assertNotNull(result);
        assertNotNull(result.metrics());
        assertTrue(result.metrics().totalHits() >= 3);
        assertFalse(result.hits().isEmpty());
        assertFalse(result.hitsOrFail().isEmpty());
        assertEquals(result.hits(), result.hitsOrFail());
        assertEquals(3, result.hits().size());
        assertTrue(result.errors().isEmpty());
    }

    @Test
    public void shouldSearchWithExplanation() {
        SearchQuery query = SearchQuery.matchPhrase("hop beer");

        SearchQueryResult result = ctx.bucket()
                .query(INDEX, query, SearchParams.build()
                        .limit(3)
                        .explain());

        for (SearchQueryRow row : result.hits()) {
            assertNotEquals(JsonObject.empty(), row.explanation());

            //sanity checks
            assertNotNull(row.id());
            assertTrue(row.index().startsWith(INDEX));
            assertTrue(row.score() > 0d);
            assertTrue(row.fields().isEmpty());
            assertTrue(row.fragments().isEmpty());
        }
        //top level sanity checks
        assertNotNull(result);
        assertNotNull(result.metrics());
        assertTrue(result.metrics().totalHits() >= 3);
        assertFalse(result.hits().isEmpty());
        assertFalse(result.hitsOrFail().isEmpty());
        assertEquals(result.hits(), result.hitsOrFail());
        assertEquals(3, result.hits().size());
        assertTrue(result.errors().isEmpty());
    }

    @Test
    public void shouldSearchWithFacets() {
        SearchQuery query = SearchQuery.match("beer");
        SearchParams searchParams = SearchParams.build()
                        .addFacets(term("foo", "name", 3),
                                date("bar", "updated", 1).addRange("old", null, "2014-01-01T00:00:00"),
                                numeric("baz", "abv", 2).addRange("strong", 4.9, null).addRange("light", null, 4.89)
                        );

        SearchQueryResult result = ctx.bucket().query(INDEX, query, searchParams);

        System.out.println(query.export(searchParams));
        System.out.println(result.facets());

        FacetResult f = result.facets().get("foo");
        assertTrue(f instanceof TermFacetResult);
        TermFacetResult foo = (TermFacetResult) f;
        assertEquals("foo", foo.name());
        assertEquals("name", foo.field());
        assertEquals(3, foo.terms().size());
        int totalFound = 0;
        for (TermRange range : foo.terms()) {
            totalFound += range.count();
        }
        assertEquals(totalFound + foo.other(), foo.total());

        f = result.facets().get("bar");
        assertTrue(f instanceof DateRangeFacetResult);
        DateRangeFacetResult bar = (DateRangeFacetResult) f;
        assertEquals("bar", bar.name());
        assertEquals("updated", bar.field());
        assertEquals(1, bar.dateRanges().size());
        totalFound = 0;
        for (DateRange range : bar.dateRanges()) {
            totalFound += range.count();
            assertEquals("old", range.name());
        }
        assertEquals(totalFound + bar.other(), bar.total());

        f = result.facets().get("baz");
        assertTrue(f instanceof NumericRangeFacetResult);
        NumericRangeFacetResult baz = (NumericRangeFacetResult) f;
        assertEquals("baz", baz.name());
        assertEquals("abv", baz.field());
        assertEquals(2, baz.numericRanges().size());
        totalFound = 0;
        for (NumericRange range : baz.numericRanges()) {
            totalFound += range.count();
            assertThat(range.name(), anyOf(is("light"), is("strong")));
        }
        assertEquals(totalFound + baz.other(), baz.total());
    }

}
