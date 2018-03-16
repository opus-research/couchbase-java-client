package com.couchbase.client.java.fts.queries;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.SearchParams;
import com.couchbase.client.java.fts.SearchQuery;
import org.junit.Test;

import static org.junit.Assert.*;

public class StringQueryTest {

    @Test
    public void shouldBuildStringQueryWithoutParams() {
        StringQuery query = SearchQuery.string("description:water and some other stuff");
        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create().put("query", "description:water and some other stuff"));
        assertEquals(expected, query.export());
    }

    @Test
    public void shouldBuildStringQueryWithParamsAndBoost() {
        StringQuery query = SearchQuery.string("q*ry", SearchParams.build().explain().limit(10)).boost(2.0);
        JsonObject expected = JsonObject.create()
            .put("query", JsonObject.create().put("query", "q*ry").put("boost", 2.0))
            .put("explain", true)
            .put("size", 10);
        assertEquals(expected, query.export());
    }

}