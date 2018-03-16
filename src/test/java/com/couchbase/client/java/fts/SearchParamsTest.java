package com.couchbase.client.java.fts;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.facet.DateRange;
import com.couchbase.client.java.fts.facet.SearchFacet;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.fts.facet.DateRange.dr;
import static com.couchbase.client.java.fts.facet.NumericRange.nr;
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
            .facets(
                SearchFacet.term("term", "somefield", 10),
                SearchFacet.dateRange("dr", "datefield", 1, dr("name", "start", "end")),
                SearchFacet.numericRange("nr", "numfield", 99, nr("name2", 0.0, 99.99))
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