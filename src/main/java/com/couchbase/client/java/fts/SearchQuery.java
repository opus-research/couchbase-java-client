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
import com.couchbase.client.java.fts.queries.FuzzyQuery;
import com.couchbase.client.java.fts.queries.MatchAllQuery;
import com.couchbase.client.java.fts.queries.MatchNoneQuery;
import com.couchbase.client.java.fts.queries.MatchPhraseQuery;
import com.couchbase.client.java.fts.queries.MatchQuery;
import com.couchbase.client.java.fts.queries.NumericRangeQuery;
import com.couchbase.client.java.fts.queries.PrefixQuery;
import com.couchbase.client.java.fts.queries.RegexpQuery;
import com.couchbase.client.java.fts.queries.StringQuery;
import com.couchbase.client.java.fts.queries.WildcardQuery;

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

    public static FuzzyQuery fuzzy(String term) {
        return fuzzy(term, SearchParams.build());
    }

    public static FuzzyQuery fuzzy(String term, SearchParams searchParams) {
        return new FuzzyQuery(term, searchParams);
    }

    public static PrefixQuery prefix(String prefix) {
        return prefix(prefix, SearchParams.build());
    }

    public static PrefixQuery prefix(String prefix, SearchParams searchParams) {
        return new PrefixQuery(prefix, searchParams);
    }

    public static RegexpQuery regexp(String regexp) {
        return regexp(regexp, SearchParams.build());
    }
    public static RegexpQuery regexp(String regexp, SearchParams searchParams) {
        return new RegexpQuery(regexp, searchParams);
    }

    public static NumericRangeQuery numericRange() {
        return numericRange(SearchParams.build());
    }

    public static NumericRangeQuery numericRange(SearchParams searchParams) {
        return new NumericRangeQuery(searchParams);
    }

    public static DateRangeQuery dateRange() {
        return dateRange(SearchParams.build());
    }

    public static DateRangeQuery dateRange(SearchParams searchParams) {
        return new DateRangeQuery(searchParams);
    }

    public static DisjunctionQuery disjuncts(SearchQuery... queries) {
        return disjuncts(SearchParams.build(), queries);
    }

    public static DisjunctionQuery disjuncts(SearchParams searchParams, SearchQuery... queries) {
        return new DisjunctionQuery(searchParams, queries);
    }

    public static ConjunctionQuery conjuncts(SearchQuery... queries) {
        return conjuncts(SearchParams.build(), queries);
    }

    public static ConjunctionQuery conjuncts(SearchParams searchParams, SearchQuery... queries) {
        return new ConjunctionQuery(searchParams, queries);
    }

    public static BooleanQuery booleans() {
        return booleans(SearchParams.build());
    }

    public static BooleanQuery booleans(SearchParams searchParams) {
        return new BooleanQuery(searchParams);
    }

    public static WildcardQuery wildcard(String wildcard) {
        return wildcard(wildcard, SearchParams.build());
    }

    public static WildcardQuery wildcard(String wildcard, SearchParams searchParams) {
        return new WildcardQuery(wildcard, searchParams);
    }

    public static DocIdQuery docId(String... docIds) {
        return docId(SearchParams.build(), docIds);
    }

    public static DocIdQuery docId(SearchParams searchParams, String... docIds) {
        return new DocIdQuery(searchParams, docIds);
    }

    public static BooleanFieldQuery booleanField(boolean value) {
        return booleanField(value, SearchParams.build());
    }

    public static BooleanFieldQuery booleanField(boolean value, SearchParams searchParams) {
        return new BooleanFieldQuery(value, searchParams);
    }

    public static MatchAllQuery matchAll() {
        return matchAll(SearchParams.build());
    }

    public static MatchAllQuery matchAll(SearchParams searchParams) {
        return new MatchAllQuery(searchParams);
    }

    public static MatchNoneQuery matchNone() {
        return matchNone(SearchParams.build());
    }

    public static MatchNoneQuery matchNone(SearchParams searchParams) {
        return new MatchNoneQuery(searchParams);
    }

    public SearchQuery boost(double boost) {
        this.boost = boost;
        return this;
    }

    public JsonObject export() {
        JsonObject result = JsonObject.create();
        searchParams.injectParams(result);
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
        return export().toString();
    }

}
