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

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.queries.BooleanFieldQuery;
import com.couchbase.client.java.fts.queries.BooleanQuery;
import com.couchbase.client.java.fts.queries.ConjunctionQuery;
import com.couchbase.client.java.fts.queries.DateRangeQuery;
import com.couchbase.client.java.fts.queries.DisjunctionQuery;
import com.couchbase.client.java.fts.queries.DocIdQuery;
import com.couchbase.client.java.fts.queries.MatchAllQuery;
import com.couchbase.client.java.fts.queries.MatchNoneQuery;
import com.couchbase.client.java.fts.queries.MatchPhraseQuery;
import com.couchbase.client.java.fts.queries.MatchQuery;
import com.couchbase.client.java.fts.queries.NumericRangeQuery;
import com.couchbase.client.java.fts.queries.PhraseQuery;
import com.couchbase.client.java.fts.queries.PrefixQuery;
import com.couchbase.client.java.fts.queries.RegexpQuery;
import com.couchbase.client.java.fts.queries.StringQuery;
import com.couchbase.client.java.fts.queries.TermQuery;
import com.couchbase.client.java.fts.queries.WildcardQuery;

public abstract class SearchQuery {

    private Double boost;

    protected SearchQuery() { }

    public static StringQuery string(String query) {
        return new StringQuery(query);
    }

    public static MatchQuery match(String match) {
        return new MatchQuery(match);
    }

    public static MatchPhraseQuery matchPhrase(String matchPhrase) {
        return new MatchPhraseQuery(matchPhrase);
    }

    public static PrefixQuery prefix(String prefix) {
        return new PrefixQuery(prefix);
    }

    public static RegexpQuery regexp(String regexp) {
        return new RegexpQuery(regexp);
    }

    public static NumericRangeQuery numericRange() {
        return new NumericRangeQuery();
    }

    public static DateRangeQuery dateRange() {
        return new DateRangeQuery();
    }

    public static DisjunctionQuery disjuncts(SearchQuery... queries) {
        return new DisjunctionQuery(queries);
    }

    public static ConjunctionQuery conjuncts(SearchQuery... queries) {
        return new ConjunctionQuery(queries);
    }

    public static BooleanQuery booleans() {
        return new BooleanQuery();
    }

    public static WildcardQuery wildcard(String wildcard) {
        return new WildcardQuery(wildcard);
    }

    public static DocIdQuery docId(String... docIds) {
        return new DocIdQuery(docIds);
    }

    public static BooleanFieldQuery booleanField(boolean value) {
        return new BooleanFieldQuery(value);
    }

    public static TermQuery term(String term) {
        return new TermQuery(term);
    }

    public static PhraseQuery phrase(String... terms) {
        return new PhraseQuery(terms);
    }

    public static MatchAllQuery matchAll() {
        return new MatchAllQuery();
    }

    public static MatchNoneQuery matchNone() {
        return new MatchNoneQuery();
    }

    public SearchQuery boost(double boost) {
        this.boost = boost;
        return this;
    }

    public JsonObject export() {
        return export(null);
    }

    public JsonObject export(SearchParams searchParams) {
        JsonObject result = JsonObject.create();
        if (searchParams != null) {
            searchParams.injectParams(result);
        }
        JsonObject query = JsonObject.create();
        injectParamsAndBoost(query);
        return result.put("query", query);
    }

    public void injectParamsAndBoost(JsonObject input) {
        if (boost != null) {
            input.put("boost", boost);
        }
        injectParams(input);
    }

    protected abstract void injectParams(JsonObject input);

    @Override
    public String toString() {
        return export(null).toString();
    }

}
