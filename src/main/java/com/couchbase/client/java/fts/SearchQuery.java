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

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.fts.queries.AbstractFtsQuery;
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

/**
 * TODO
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class SearchQuery {

    private final String indexName;
    private final AbstractFtsQuery queryPart;
    private final SearchParams params;

    public SearchQuery(String indexName, AbstractFtsQuery queryPart) {
        this(indexName, queryPart, null);
    }

    public SearchQuery(String indexName, AbstractFtsQuery queryPart, SearchParams params) {
        this.indexName = indexName;
        this.queryPart = queryPart;
        this.params = params;
    }

    public String indexName() {
        return indexName;
    }

    public AbstractFtsQuery query() {
        return queryPart;
    }

    public SearchParams params() {
        return params;
    }

    /* ===============================
     * Builder methods for FTS queries
     * =============================== */

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

    public static DisjunctionQuery disjuncts(AbstractFtsQuery... queries) {
        return new DisjunctionQuery(queries);
    }

    public static ConjunctionQuery conjuncts(AbstractFtsQuery... queries) {
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
}
