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