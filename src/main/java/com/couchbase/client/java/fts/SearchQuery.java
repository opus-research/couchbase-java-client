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
package com.couchbase.client.java.fts;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.queries.MatchPhraseQuery;
import com.couchbase.client.java.fts.queries.MatchQuery;
import com.couchbase.client.java.fts.queries.StringQuery;

public abstract class SearchQuery {

    private final SearchParams searchParams;
    private Double boost;

    protected SearchQuery(final SearchParams searchParams) {
        this.searchParams = searchParams;
    }

    public static StringQuery string(String query) {
        return string(query, SearchParams.build());
    }

    public static StringQuery string(String query, SearchParams searchParams) {
        return new StringQuery(query, searchParams);
    }

    public static MatchQuery match(String match) {
        return match(match, SearchParams.build());
    }

    public static MatchQuery match(String match, SearchParams searchParams) {
        return new MatchQuery(match, searchParams);
    }

    public static MatchPhraseQuery matchPhrase(String matchPhrase) {
        return matchPhrase(matchPhrase, SearchParams.build());
    }

    public static MatchPhraseQuery matchPhrase(String matchPhrase, SearchParams searchParams) {
        return new MatchPhraseQuery(matchPhrase, searchParams);
    }

    public SearchQuery boost(double boost) {
        this.boost = boost;
        return this;
    }

    public JsonObject export() {
        JsonObject result = JsonObject.create();
        searchParams.injectParams(result);
        JsonObject query = JsonObject.create();
        injectParams(query);
        return result.put("query", query);
    }

    protected void injectParams(JsonObject input) {
        if (boost != null) {
            input.put("boost", boost);
        }
    }

    @Override
    public String toString() {
        return export().toString();
    }

}
